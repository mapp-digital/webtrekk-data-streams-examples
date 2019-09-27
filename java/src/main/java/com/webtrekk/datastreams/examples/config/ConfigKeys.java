package com.webtrekk.datastreams.examples.config;

public class ConfigKeys {

    public static final String ClientId = "client-id";
    public static final String GroupId = "consumer-group";
    public static final String Endpoints = "endpoints";
    public static final String AutoOffsetResetPolicy = "auto-offset-reset-policy";

    public static final String SecurityProtocol = "security-protocol";
    public static final String SecuritySaslMechanism = "security-sasl-mechanism";

    public static final String SecurityScramUsername = "username";
    public static final String SecurityScramPassword = "password";

    public static final String Topic = "topic";

    public static class Streams {
        public static final String NumOfThreads = "streams-num-of-threads";
        public static final String DeserializationExceptionHandler = "streams-deserialization-error-handler";
    }

}
