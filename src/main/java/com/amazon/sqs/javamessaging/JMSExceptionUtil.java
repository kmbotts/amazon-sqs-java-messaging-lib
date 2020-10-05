package com.amazon.sqs.javamessaging;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import java.util.function.Supplier;

class JMSExceptionUtil {

    static Supplier<JMSException> UnsupportedMethod() {
        return () -> new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    static Supplier<JMSException> NullAmazonSqsClient() {
        return () -> new JMSException("Amazon SQS Client is null!");
    }

    static Supplier<JMSException> DestinationTypeMismatch() {
        return () -> new JMSException("Actual type of Destination/Queue has to be SQSQueueDestination");
    }

    static Supplier<JMSException> MessageSelectorUnsupported() {
        return () -> new JMSException("SQSSession does not support MessageSelector. This should be null.");
    }

    static Supplier<IllegalStateException> DestinationAlreadySet() {
        return () -> new IllegalStateException("MessageProducer already specified a destination at creation time.");
    }

    static JMSException convertExceptionToJMSException(Exception e) {
        JMSException ex = new JMSException(e.getMessage());
        ex.initCause(e);
        return ex;
    }

    static MessageFormatException convertExceptionToMessageFormatException(Exception e) {
        MessageFormatException ex = new MessageFormatException(e.getMessage());
        ex.initCause(e);
        return ex;
    }

    static JMSException conversionUnsupportedException(Class<?> toClass, Object value) {
        return new JMSException(toClass.getName() + " conversion not supported for " + value.getClass().getName());
    }
}
