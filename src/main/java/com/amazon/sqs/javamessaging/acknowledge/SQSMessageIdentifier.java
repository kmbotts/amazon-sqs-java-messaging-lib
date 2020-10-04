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
package com.amazon.sqs.javamessaging.acknowledge;

import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.util.StringUtils;
import lombok.Builder;
import lombok.Data;

import javax.jms.JMSException;

/**
 * Identifies an SQS message, when (negative)acknowledging the message
 */
@Data
public class SQSMessageIdentifier {

    // The queueUrl where the message was sent or received from
    private final String queueUrl;

    // The receipt handle returned after the delivery of the message from SQS
    private final String receiptHandle;

    // The SQS message id assigned on send.
    private final String SQSMessageID;

    // The group id to which the message belongs
    private final String groupId;

    @Builder
    SQSMessageIdentifier(String queueUrl, String receiptHandle, String SQSMessageID, String groupId) {
        this.queueUrl = queueUrl;
        this.receiptHandle = receiptHandle;
        this.SQSMessageID = SQSMessageID;
        this.groupId = StringUtils.isNullOrEmpty(groupId) || groupId.trim().isEmpty() ? null : groupId.trim();
    }

    public static SQSMessageIdentifier fromSQSMessage(SQSMessage sqsMessage) throws JMSException {
        return new SQSMessageIdentifier(sqsMessage.getQueueUrl(), sqsMessage.getReceiptHandle(), sqsMessage.getSQSMessageID(), sqsMessage.getSQSMessageGroupId());
    }
}
