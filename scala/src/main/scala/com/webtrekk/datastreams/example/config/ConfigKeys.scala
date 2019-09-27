package com.webtrekk.datastreams.example.config

object ConfigKeys {
  val ClientId = "client-id"
  val GroupId = "consumer-group"
  val Endpoints = "endpoints"
  val AutoOffsetResetPolicy = "auto-offset-reset-policy"

  val SecurityProtocol = "security-protocol"
  val SecuritySaslMechanism = "security-sasl-mechanism"

  val SecurityScramUsername = "username"
  val SecurityScramPassword = "password"

  val Topic = "topic"

  object Streams {
    val NumOfThreads = "streams-num-of-threads"
    val DeserializationExceptionHandler = "streams-deserialization-error-handler"
  }
}