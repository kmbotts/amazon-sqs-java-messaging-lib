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

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazonaws.services.sqs.AmazonSQS;

import java.util.concurrent.ThreadFactory;

/**
 * A client uses a MessageConsumer object to receive messages from a
 * destination. A MessageConsumer object is created by passing a Destination
 * object to a message-consumer creation method supplied by a session.
 * <p>
 * This message consumer does not support message selectors
 * <p>
 * A client may either synchronously receive a message consumer's messages or
 * have the consumer asynchronously deliver them as they arrive via registering
 * a MessageListener object.
 * <p>
 * The message consumer creates a background thread to prefetch the messages to
 * improve the <code>receive</code> turn-around times.
 */
public class SQSMessageConsumer extends AbstractMessageConsumer<AmazonSQS> {

    SQSMessageConsumer(AbstractConnection<AmazonSQS> parentSQSConnection,
                       AbstractSession<AmazonSQS> parentSQSSession,
                       SQSSessionCallbackScheduler<AmazonSQS> sqsSessionRunnable,
                       SQSQueueDestination destination,
                       Acknowledger acknowledger,
                       NegativeAcknowledger<AmazonSQS> negativeAcknowledger,
                       ThreadFactory threadFactory) {

        super(parentSQSConnection, parentSQSSession, sqsSessionRunnable, destination, acknowledger, negativeAcknowledger, threadFactory);
    }

    SQSMessageConsumer(AbstractConnection<AmazonSQS> parentSQSConnection,
                       AbstractSession<AmazonSQS> parentSQSSession,
                       SQSSessionCallbackScheduler<AmazonSQS> sqsSessionRunnable,
                       SQSQueueDestination destination,
                       Acknowledger acknowledger,
                       NegativeAcknowledger<AmazonSQS> negativeAcknowledger,
                       ThreadFactory threadFactory,
                       SQSMessageConsumerPrefetch<AmazonSQS> sqsMessageConsumerPrefetch) {

        super(parentSQSConnection, parentSQSSession, sqsSessionRunnable, destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch);
    }
}
