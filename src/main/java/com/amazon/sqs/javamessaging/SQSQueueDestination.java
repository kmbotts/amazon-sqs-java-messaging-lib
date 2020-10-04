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

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import javax.jms.JMSException;
import javax.jms.Queue;

/**
 * A SQSQueueDestination object encapsulates a queue name and SQS specific queue
 * URL. This is the way a client specifies the identity of a queue to JMS API
 * methods.
 */
public class SQSQueueDestination implements Queue {

    private final String queueName;

    @Getter
    private final String queueUrl;

    @Getter
    private final boolean isFifo;

    @Builder(access = AccessLevel.PACKAGE)
    SQSQueueDestination(String queueName, String queueUrl) {
        this.queueName = queueName;
        this.queueUrl = queueUrl;
        this.isFifo = this.queueName.endsWith(".fifo");
    }

    @Override
    @SuppressWarnings("RedundantThrows")
    public String getQueueName() throws JMSException {
        return this.queueName;
    }
}
