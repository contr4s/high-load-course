package ru.quipy.payments.logic

class PaymentSubmissionThrottledException(
    val retryAfterMillis: Long,
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
