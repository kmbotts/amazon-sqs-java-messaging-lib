/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging;

import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import lombok.AccessLevel;
import lombok.Builder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;

/**
 * A client uses a MessageProducer object to send messages to a queue
 * destination. A MessageProducer object is created by passing a Destination
 * object to a message-producer creation method supplied by a session.
 * <p>
 * A client also has the option of creating a message producer without supplying
 * a queue destination. In this case, a destination must be provided with every send
 * operation.
 * <p>
 */
public class SQSMessageProducer extends AbstractMessageProducer {
    private static final Log LOG = LogFactory.getLog(SQSMessageProducer.class);

    @Builder(access = AccessLevel.PACKAGE)
    SQSMessageProducer(AbstractSQSClientWrapper sqsClientWrapper,
                       AbstractSession session,
                       Destination destination) {
        super(sqsClientWrapper, session, destination);
    }

    @Override
    protected void sendMessageInternal(Queue queue, Message message, CompletionListener listener) throws JMSException {
        checkClosed();
        SQSQueueDestination sqsQueueDestination = (SQSQueueDestination) queue;
        SQSMessage sqsMessage = checkMessageFormat(message);

        SendMessageRequest sendMessageRequest = getSendMessageRequest(sqsQueueDestination, sqsMessage);
        SendMessageResult sendMessageResult = getSqsClientWrapper().sendMessage(sendMessageRequest);
        String messageId = sendMessageResult.getMessageId();
        LOG.info("Message sent to SQS with SQS-assigned messageId: " + messageId);
        /* TODO: Do not support disableMessageID for now. */
        sqsMessage.setSQSMessageId(messageId);

        // if the message was sent to FIFO queue, the sequence number will be
        // set in the response
        // pass it to JMS user through provider specific JMS property
        if (sendMessageResult.getSequenceNumber() != null) {
            sqsMessage.setSequenceNumber(sendMessageResult.getSequenceNumber());
        }
    }
}
