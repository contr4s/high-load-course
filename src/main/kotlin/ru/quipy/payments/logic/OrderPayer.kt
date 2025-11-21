package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val MIN_BUFFER_WINDOW_MILLIS = 250L
        private const val BURST_BUFFER_WINDOW_MILLIS = 3000L
        private const val MAX_WORKER_POOL_SIZE = 128
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

    private val workerParallelism = max(1, min(maxParallelRequests, MAX_WORKER_POOL_SIZE.coerceAtLeast(1)))

    private val bufferWindowSeconds = max(
        (BURST_BUFFER_WINDOW_MILLIS.coerceAtLeast(MIN_BUFFER_WINDOW_MILLIS)).toDouble() / 1000.0,
        MIN_BUFFER_WINDOW_MILLIS.toDouble() / 1000.0
    )

    private val drainRatePerSecond = effectiveRateLimitPerSecond.coerceAtLeast(1)

    private val burstBufferCapacity = ceil(drainRatePerSecond * bufferWindowSeconds).toInt()

    private val queueCapacity = max(1, burstBufferCapacity)

    private val submissionRateLimiter = SlidingWindowRateLimiter(
        rate = effectiveRateLimitPerSecond.coerceAtLeast(1).toLong(),
        window = Duration.ofSeconds(1)
    )

    private val paymentExecutor = ThreadPoolExecutor(
        workerParallelism,
        workerParallelism,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler(Duration.ofMillis(BURST_BUFFER_WINDOW_MILLIS.coerceAtLeast(MIN_BUFFER_WINDOW_MILLIS)))
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val acceptedAt = System.currentTimeMillis()
        val task = PaymentTask(orderId, amount, paymentId, deadline, acceptedAt)
        try {
            paymentExecutor.execute { handlePayment(task) }
            } catch (ex: RejectedExecutionException) {
                val retryAfter = BURST_BUFFER_WINDOW_MILLIS.coerceAtLeast(MIN_BUFFER_WINDOW_MILLIS)
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

        var now = System.currentTimeMillis()
        if (now >= task.deadline) {
            logExpiredInBuffer(task, now)
            return
        }

        submissionRateLimiter.tickBlocking()

        now = System.currentTimeMillis()
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