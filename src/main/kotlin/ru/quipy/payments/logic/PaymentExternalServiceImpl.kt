package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val REQUEST_TIMEOUT_MS = 50_000L
        const val MAX_RETRIES = 4
        const val MIN_TIME_FOR_RETRY = 1000L

        private val httpClient: HttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .executor(Executors.newFixedThreadPool(100))
            .build()
        
        private val paymentScope = CoroutineScope(
            Executors.newFixedThreadPool(100).asCoroutineDispatcher() + SupervisorJob()
        )
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        bucketMaxCapacity = rateLimitPerSec,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    private val ongoingWindowSemaphore = Semaphore(parallelRequests)

    private val expiredPaymentsCounter: Counter = Counter.builder("payment_expired_total")
        .description("Total number of payments that expired before submission due to deadline")
        .tag("account", accountName)
        .register(meterRegistry)
    
    private val retriesCounter: Counter = Counter.builder("payment_retries_total")
        .description("Total number of payment retry attempts")
        .tag("account", accountName)
        .register(meterRegistry)
    
    private val timeoutCounter: Counter = Counter.builder("payment_timeout_total")
        .description("Total number of payment timeouts")
        .tag("account", accountName)
        .register(meterRegistry)
    
    private val requestLatencyTimer: Timer = Timer.builder("payment_request_latency")
        .description("Payment request latency")
        .tag("account", accountName)
        .publishPercentiles(0.5, 0.85, 0.95, 0.99)
        .register(meterRegistry)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        paymentScope.launch {
            performPaymentSuspend(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private suspend fun performPaymentSuspend(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val currentTime = now()
        
        if (currentTime >= deadline) {
            logger.error("[$accountName] Payment $paymentId expired before processing (current: $currentTime, deadline: $deadline)")
            expiredPaymentsCounter.increment()
            
            paymentESService.update(paymentId) {
                it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Payment expired before submission (deadline exceeded)")
            }
            return
        }

        val timeToWait = deadline - now() - MIN_TIME_FOR_RETRY
        val acquired = withTimeoutOrNull(timeToWait) {
            ongoingWindowSemaphore.acquire()
            true
        } ?: false
        
        if (!acquired) {
            logger.warn("[$accountName] Payment $paymentId rejected: ongoing window full after waiting ${timeToWait}ms")
            expiredPaymentsCounter.increment()
            
            paymentESService.update(paymentId) {
                it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Ongoing window is full after timeout")
            }
            return
        }

        try {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            executeWithRetry(paymentId, transactionId, amount, deadline, paymentStartedAt, 0)
        } finally {
            ongoingWindowSemaphore.release()
        }
    }

    private suspend fun executeWithRetry(
        paymentId: UUID, 
        transactionId: UUID, 
        amount: Int, 
        deadline: Long,
        paymentStartedAt: Long,
        retryCount: Int
    ) {
        val currentTime = now()
        val timeRemaining = deadline - currentTime
        
        if (timeRemaining < MIN_TIME_FOR_RETRY) {
            logger.warn("[$accountName] Not enough time for retry, remaining: ${timeRemaining}ms, payment: $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Not enough time for retry")
            }
            return
        }
        
        val rateLimitAcquired = rateLimiter.tickWithTimeout(timeRemaining - MIN_TIME_FOR_RETRY, TimeUnit.MILLISECONDS)
        if (!rateLimitAcquired) {
            logger.warn("[$accountName] Payment $paymentId retry rejected: rate limit exceeded (attempt ${retryCount + 1})")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceeded during retry attempt ${retryCount + 1}")
            }
            return
        }

        val requestStartTime = now()
        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMillis(REQUEST_TIMEOUT_MS))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        try {
            val response = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            val requestDuration = now() - requestStartTime
            requestLatencyTimer.record(requestDuration, TimeUnit.MILLISECONDS)
            
            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
            
            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
        } catch (e: Exception) {
            val requestDuration = now() - requestStartTime
            requestLatencyTimer.record(requestDuration, TimeUnit.MILLISECONDS)
            
            val isRetryableError = e is HttpTimeoutException ||
                e is java.net.ConnectException ||
                e is java.net.SocketException ||
                e is java.io.IOException ||
                e.cause is java.net.SocketException ||
                e.cause is java.net.ConnectException
            
            if (isRetryableError) {
                timeoutCounter.increment()
                logger.warn("[$accountName] Retryable error (attempt ${retryCount + 1}) for txId: $transactionId, payment: $paymentId: ${e.javaClass.simpleName}: ${e.message}")
                
                if (retryCount < MAX_RETRIES && now() + MIN_TIME_FOR_RETRY < deadline) {
                    retriesCounter.increment()
                    logger.info("[$accountName] Retrying payment (attempt ${retryCount + 2}) for txId: $transactionId, payment: $paymentId")

                    val backoffMs = (100L * (1 shl retryCount)).coerceAtMost(2000L)
                    delay(backoffMs + (Math.random() * 100).toLong())
                    executeWithRetry(paymentId, transactionId, amount, deadline, paymentStartedAt, retryCount + 1)
                    return
                }

                logger.warn("[$accountName] Not enough time for retry, remaining: ${deadline - now()}ms, (attempt ${retryCount + 1}) for txId: $transactionId, payment: $paymentId: ${e.javaClass.simpleName}:")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Connection error after ${retryCount + 1} attempts: ${e.message}")
                }
            } else {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun rateLimitPerSecond() = properties.rateLimitPerSec

    override fun parallelRequests() = properties.parallelRequests

    override fun averageProcessingTime() = properties.averageProcessingTime

}

public fun now() = System.currentTimeMillis()