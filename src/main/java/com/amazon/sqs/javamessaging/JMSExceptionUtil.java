package com.amazon.sqs.javamessaging;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import java.util.function.Supplier;

class JMSExceptionUtil {

    public static Supplier<JMSException> UnsupportedMethod() {
        return () -> new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    public static Supplier<JMSException> NullAmazonSqsClient() {
        return () -> new JMSException("Amazon SQS Client is null!");
    }

    public static Supplier<JMSException> DestinationTypeMismatch() {
        return () -> new JMSException("Actual type of Destination/Queue has to be SQSQueueDestination");
    }

    public static Supplier<JMSException> MessageSelectorUnsupported() {
        return () -> new JMSException("SQSSession does not support MessageSelector. This should be null.");
    }

    public static Supplier<IllegalStateException> DestinationAlreadySet() {
        return () -> new IllegalStateException("MessageProducer already specified a destination at creation time.");
    }
}
