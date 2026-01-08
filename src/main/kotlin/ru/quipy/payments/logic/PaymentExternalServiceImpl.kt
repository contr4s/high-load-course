package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
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

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val REQUEST_TIMEOUT_MS = 12_000L
        const val MAX_RETRIES = 4
        const val MAX_CONNECTIONS = 10000
        const val MIN_TIME_FOR_RETRY = 1000

        private val sharedConnectionPool = ConnectionPool(
            maxIdleConnections = 500,
            keepAliveDuration = 5,
            timeUnit = TimeUnit.MINUTES
        )
        
        private val sharedDispatcher = Dispatcher().apply {
            maxRequests = MAX_CONNECTIONS
            maxRequestsPerHost = MAX_CONNECTIONS
        }
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectionPool(sharedConnectionPool)
        .dispatcher(sharedDispatcher)
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .writeTimeout(5, TimeUnit.SECONDS)
        .callTimeout(REQUEST_TIMEOUT_MS + 2_000, TimeUnit.MILLISECONDS)
        .retryOnConnectionFailure(true)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val ongoingWindow = OngoingWindow(maxWinSize = parallelRequests)

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

        val transactionId = UUID.randomUUID()
        val currentTime = now()
        if (currentTime >= deadline) {
            logger.error("[$accountName] Payment $paymentId expired before rate limiter (current: $currentTime, deadline: $deadline)")
            expiredPaymentsCounter.increment()
            
            paymentESService.update(paymentId) {
                it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Payment expired before submission (deadline exceeded)")
            }
            return
        }

        rateLimiter.tickBlocking()

        // Проверяем deadline после ожидания в rate limiter
        val afterRateLimiterTime = now()
        if (afterRateLimiterTime >= deadline) {
            expiredPaymentsCounter.increment()
            
            paymentESService.update(paymentId) {
                it.logSubmission(success = false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Payment expired after rate limiter wait (deadline exceeded)")
            }
            return
        }

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        ongoingWindow.acquire()

        val afterWindowTime = now()
        if (afterWindowTime >= deadline) {
            ongoingWindow.release()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Payment expired while waiting in ongoing window")
            }
            return
        }

        try {
            executeWithRetry(paymentId, transactionId, amount, deadline)
        } finally {
            ongoingWindow.release()
        }
    }

    private fun executeWithRetry(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long) {
        var retryCount = 0
        
        while (retryCount <= MAX_RETRIES) {
            val currentTime = now()
            val timeRemaining = deadline - currentTime
            
            if (timeRemaining < MIN_TIME_FOR_RETRY) {
                logger.warn("[$accountName] Not enough time for retry, remaining: ${timeRemaining}ms, payment: $paymentId")
                if (retryCount != 0) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Not enough time for retry")
                    }
                    return
                }
            }
            
            val requestStartTime = now()
            try {
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                client.newCall(request).execute().use { response ->
                    val requestDuration = now() - requestStartTime
                    requestLatencyTimer.record(requestDuration, TimeUnit.MILLISECONDS)
                    
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return
                }
            } catch (e: Exception) {
                val requestDuration = now() - requestStartTime
                requestLatencyTimer.record(requestDuration, TimeUnit.MILLISECONDS)
                
                val isRetryableError = e is SocketTimeoutException ||
                    e is java.io.InterruptedIOException ||
                    e is java.net.SocketException ||
                    e is java.net.BindException ||
                    e.cause is java.net.SocketException ||
                    e.cause is java.net.BindException
                
                if (isRetryableError) {
                    timeoutCounter.increment()
                    logger.warn("[$accountName] Retryable error (attempt ${retryCount + 1}) for txId: $transactionId, payment: $paymentId: ${e.javaClass.simpleName}: ${e.message}")
                    
                    if (retryCount < MAX_RETRIES && now() + REQUEST_TIMEOUT_MS < deadline) {
                        retryCount++
                        retriesCounter.increment()
                        Thread.sleep(50)
                        logger.info("[$accountName] Retrying payment (attempt ${retryCount + 1}) for txId: $transactionId, payment: $paymentId")
                        continue
                    }
                    
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Connection error after ${retryCount + 1} attempts: ${e.message}")
                    }
                    return
                } else {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    return
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