package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.max
import kotlin.random.Random

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val BASE_RETRY_AFTER_MILLIS = 50L
        private const val MAX_RETRY_AFTER_MILLIS = 200L
        private const val JITTER_FACTOR = 0.2
    }

    private val enabledAccounts = paymentAccounts.filter { it.isEnabled() }

    private val effectiveRateLimitPerSecond = enabledAccounts
        .takeIf { it.isNotEmpty() }
        ?.sumOf { it.rateLimitPerSecond().coerceAtLeast(1) }
        ?: 1

    private val maxParallelRequests = enabledAccounts
        .takeIf { it.isNotEmpty() }
        ?.sumOf { it.parallelRequests().coerceAtLeast(1) }
        ?: 1

    private val avgProcessingTimeMs = enabledAccounts
        .takeIf { it.isNotEmpty() }
        ?.map { it.averageProcessingTime().toMillis() }
        ?.average()?.toLong()
        ?: 1000L

    private val workerParallelism = max(1, maxParallelRequests)

    private val entryRateLimiter = TokenBucketRateLimiter(
        rate = effectiveRateLimitPerSecond,
        bucketMaxCapacity = effectiveRateLimitPerSecond * 2,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    private val queueCapacity = maxParallelRequests * 2

    private val paymentExecutor = ThreadPoolExecutor(
        workerParallelism,
        workerParallelism,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler(Duration.ofMillis(BASE_RETRY_AFTER_MILLIS))
    )

    private fun calculateRetryAfter(deadline: Long): Long {
        val now = System.currentTimeMillis()
        val remainingTime = deadline - now
        
        if (remainingTime <= avgProcessingTimeMs) {
            return BASE_RETRY_AFTER_MILLIS
        }

        val baseRetry = (remainingTime - avgProcessingTimeMs) / 3
        val jitter = 1.0 + Random.nextDouble() * JITTER_FACTOR
        return (baseRetry * jitter).toLong().coerceIn(BASE_RETRY_AFTER_MILLIS, MAX_RETRY_AFTER_MILLIS)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val now = System.currentTimeMillis()
        
        if (now >= deadline) {
            logger.warn("Payment {} for order {} already expired at entry (deadline={}, now={})", paymentId, orderId, deadline, now)
            paymentESService.create { it.create(paymentId, orderId, amount) }
            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(false, transactionId, now, Duration.ZERO)
            }
            paymentESService.update(paymentId) {
                it.logProcessing(false, now, transactionId, reason = "Payment expired before processing (deadline exceeded at entry)")
            }
            return now
        }
        
        if (!entryRateLimiter.tick()) {
            val remainingTime = deadline - System.currentTimeMillis()
            if (remainingTime < avgProcessingTimeMs / 2) {
                logger.warn("Payment {} for order {} rejected: not enough time for retry (remaining={}ms)", paymentId, orderId, remainingTime)
                paymentESService.create { it.create(paymentId, orderId, amount) }
                val transactionId = UUID.randomUUID()
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, System.currentTimeMillis(), Duration.ZERO)
                }
                paymentESService.update(paymentId) {
                    it.logProcessing(false, System.currentTimeMillis(), transactionId, reason = "Payment rejected: not enough time remaining for retry")
                }
                return System.currentTimeMillis()
            }
            
            val retryAfter = calculateRetryAfter(deadline)
            logger.debug("Rate limit exceeded for payment {} order {}, returning 429 with retry-after {} ms", paymentId, orderId, retryAfter)
            throw PaymentSubmissionThrottledException(
                retryAfterMillis = retryAfter,
                message = "Rate limit exceeded. Retry later.",
            )
        }

        val acceptedAt = System.currentTimeMillis()
        val task = PaymentTask(orderId, amount, paymentId, deadline, acceptedAt)
        try {
            paymentExecutor.execute { handlePayment(task) }
        } catch (ex: RejectedExecutionException) {
            val remainingTime = deadline - System.currentTimeMillis()
            if (remainingTime < avgProcessingTimeMs / 2) {
                logger.warn("Payment {} for order {} rejected on buffer overflow: not enough time for retry (remaining={}ms)", paymentId, orderId, remainingTime)
                paymentESService.create { it.create(paymentId, orderId, amount) }
                val transactionId = UUID.randomUUID()
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, System.currentTimeMillis(), Duration.ZERO)
                }
                paymentESService.update(paymentId) {
                    it.logProcessing(false, System.currentTimeMillis(), transactionId, reason = "Payment rejected on buffer overflow: not enough time remaining")
                }
                return System.currentTimeMillis()
            }
            
            val retryAfter = calculateRetryAfter(deadline)
            logger.warn(
                "Failed to enqueue payment {} for order {} due to saturated buffer, retry-after {} ms",
                paymentId,
                orderId,
                retryAfter,
            )
            throw PaymentSubmissionThrottledException(
                retryAfterMillis = retryAfter,
                message = "Payment submission buffer is saturated. Retry later.",
                cause = ex,
            )
        }
        return acceptedAt
    }

    private fun handlePayment(task: PaymentTask) {
        val createdEvent = paymentESService.create {
            it.create(task.paymentId, task.orderId, task.amount)
        }
        logger.trace("Payment ${createdEvent.paymentId} for order ${task.orderId} created.")

        val now = System.currentTimeMillis()
        if (now >= task.deadline) {
            logExpiredInBuffer(task, now)
            return
        }

        paymentService.submitPaymentRequest(task.paymentId, task.amount, task.acceptedAt, task.deadline)
    }

    private fun logExpiredInBuffer(task: PaymentTask, now: Long) {
        val transactionId = UUID.randomUUID()
        val spentInQueue = Duration.ofMillis((now - task.acceptedAt).coerceAtLeast(0))

        paymentESService.update(task.paymentId) {
            it.logSubmission(false, transactionId, now, spentInQueue)
        }
        paymentESService.update(task.paymentId) {
            it.logProcessing(false, now, transactionId, reason = "Payment expired while waiting in buffer (deadline=${task.deadline})")
        }

        logger.warn(
            "Payment {} expired before reaching external provider (deadline {}, now {}, waited {} ms)",
            task.paymentId,
            task.deadline,
            now,
            spentInQueue.toMillis(),
        )
    }

    private data class PaymentTask(
        val orderId: UUID,
        val amount: Int,
        val paymentId: UUID,
        val deadline: Long,
        val acceptedAt: Long,
    )
}