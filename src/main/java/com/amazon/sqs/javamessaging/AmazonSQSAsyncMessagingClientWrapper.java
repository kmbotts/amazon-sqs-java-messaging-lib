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

import com.amazon.sqs.javamessaging.acknowledge.SendMessageAsyncHandler;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import javax.jms.JMSException;

/**
 * This is a JMS Wrapper of <code>AmazonSQSClient</code>. This class changes all
 * <code>AmazonServiceException</code> and <code>AmazonClientException</code> into
 * JMSException/JMSSecurityException.
 */
public class AmazonSQSAsyncMessagingClientWrapper extends AbstractSQSClientWrapper {

    private final AmazonSQSAsync amazonSQSAsync;

    protected AmazonSQSAsyncMessagingClientWrapper(AmazonSQSAsync amazonSQSAsync) {
        this(amazonSQSAsync, null);
    }

    protected AmazonSQSAsyncMessagingClientWrapper(AmazonSQSAsync amazonSQSAsync, AWSCredentialsProvider credentialsProvider) {
        super(credentialsProvider);
        this.amazonSQSAsync = amazonSQSAsync;
    }

    @Override
    public AmazonSQSAsync getClient() {
        return this.amazonSQSAsync;
    }

    public void sendMessageAsync(SendMessageRequest request, SendMessageAsyncHandler asyncHandler) throws JMSException {
        try {
            prepareRequest(request);
            getClient().sendMessageAsync(request, asyncHandler);
        } catch (AmazonClientException e) {
            throw handleException(e, "sendMessage");
        }
    }
}
