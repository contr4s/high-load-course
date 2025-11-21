package ru.quipy.apigateway

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import ru.quipy.payments.logic.PaymentSubmissionThrottledException
import kotlin.math.max

@RestControllerAdvice
class ApiExceptionHandler {

    private val logger = LoggerFactory.getLogger(ApiExceptionHandler::class.java)

    @ExceptionHandler(PaymentSubmissionThrottledException::class)
    fun handlePaymentThrottled(ex: PaymentSubmissionThrottledException): ResponseEntity<ErrorResponse> {
        val retryAfterSeconds = max(1L, (ex.retryAfterMillis + 999) / 1000)
        logger.warn("Payment submission throttled, retry after {}s", retryAfterSeconds)

        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", retryAfterSeconds.toString())
            .body(ErrorResponse(ex.message ?: "Payment submission throttled", retryAfterSeconds))
    }

    data class ErrorResponse(
        val message: String,
        val retryAfterSeconds: Long,
    )
}
