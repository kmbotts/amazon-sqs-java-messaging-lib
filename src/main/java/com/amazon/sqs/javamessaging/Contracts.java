package com.amazon.sqs.javamessaging;

import javax.jms.JMSException;

import java.util.List;

class Contracts {
    interface Acknowledger {

        /**
         * Generic Acknowledge method. This method will delete message(s) in SQS Queue.
         *
         * @param message message to acknowledge.
         * @throws JMSException exception
         */
        void acknowledge(SQSMessage message) throws JMSException;

        /**
         * Used when receiving messages. Depending on acknowledge mode this will
         * help create list of message backlog.
         *
         * @param message notify acknowledger message is received
         * @throws JMSException exception
         */
        void notifyMessageReceived(SQSMessage message) throws JMSException;

        /**
         * Used in negative acknowledge. Gets all delivered but not acknowledged
         * messages.
         */
        List<SQSMessageIdentifier> getUnAckMessages();

        /**
         * Deletes all not acknowledged delivered messages.
         */
        void forgetUnAckMessages();

    }

    /**
     * This interface is helper to notify when the prefetchThread should be resuming
     * messages.
     */
    interface PrefetchManager {

        /**
         * Notify the prefetchThread that the message is dispatched from
         * messageQueue when user calls for receive or message listener onMessage is
         * called.
         */
        void messageDispatched();

        /**
         * Notify the prefetchThread that the message listener has finished with any
         * previous message and is ready to accept another.
         */
        void messageListenerReady();

        /**
         * This is used to determine the state of the consumer, when the message
         * listener scheduler is processing the messages.
         *
         * @return The message consumer, which owns the prefetchThread
         */
        AbstractMessageConsumer getMessageConsumer();
    }
}
