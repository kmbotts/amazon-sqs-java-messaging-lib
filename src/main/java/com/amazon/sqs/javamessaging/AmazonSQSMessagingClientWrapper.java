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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;

/**
 * This is a JMS Wrapper of <code>AmazonSQSClient</code>. This class changes all
 * <code>AmazonServiceException</code> and <code>AmazonClientException</code> into
 * JMSException/JMSSecurityException.
 */
public class AmazonSQSMessagingClientWrapper extends AbstractSQSClientWrapper {

    private final AmazonSQS amazonSQS;

    protected AmazonSQSMessagingClientWrapper(AmazonSQS amazonSQS) {
        this(amazonSQS, null);
    }

    protected AmazonSQSMessagingClientWrapper(AmazonSQS amazonSQS, AWSCredentialsProvider credentialsProvider) {
        super(credentialsProvider);
        this.amazonSQS = amazonSQS;
    }

    @Override
    public AmazonSQS getClient() {
        return this.amazonSQS;
    }
}
