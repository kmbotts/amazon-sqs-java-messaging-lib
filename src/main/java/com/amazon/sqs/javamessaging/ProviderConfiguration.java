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

import com.amazon.sqs.javamessaging.util.MessagingClientThreadFactory;

import java.util.Optional;

public class ProviderConfiguration {
    private int numberOfMessagesToPrefetch;
    private MessagingClientThreadFactory sessionThreadFactory;
    private MessagingClientThreadFactory consumerPrefetchThreadFactory;

    public ProviderConfiguration() {
        // Set default numberOfMessagesToPrefetch to MIN_BATCH.
        this.numberOfMessagesToPrefetch = SQSMessagingClientConstants.MIN_BATCH;
        // Set default session thread factory
        this.sessionThreadFactory = SQSSession.SESSION_THREAD_FACTORY;
        this.consumerPrefetchThreadFactory = SQSSession.CONSUMER_PREFETCH_THREAD_FACTORY;
    }

    public int getNumberOfMessagesToPrefetch() {
        return numberOfMessagesToPrefetch;
    }

    public void setNumberOfMessagesToPrefetch(int numberOfMessagesToPrefetch) {
        if (numberOfMessagesToPrefetch < SQSMessagingClientConstants.MIN_PREFETCH) {
            throw new IllegalArgumentException(String.format("Invalid prefetch size. Provided value '%1$s' cannot be smaller than '%2$s'", numberOfMessagesToPrefetch, SQSMessagingClientConstants.MIN_PREFETCH));
        }
        this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;
    }

    public ProviderConfiguration withNumberOfMessagesToPrefetch(int numberOfMessagesToPrefetch) {
        setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
        return this;
    }

    public MessagingClientThreadFactory getSessionThreadFactory() {
        return sessionThreadFactory;
    }

    public void setSessionThreadFactory(MessagingClientThreadFactory sessionThreadFactory) {
        this.sessionThreadFactory = Optional.ofNullable(sessionThreadFactory).orElse(this.sessionThreadFactory);
    }

    public ProviderConfiguration withSessionThreadFactory(MessagingClientThreadFactory sessionThreadFactory) {
        setSessionThreadFactory(sessionThreadFactory);
        return this;
    }

    public MessagingClientThreadFactory getConsumerPrefetchThreadFactory() {
        return consumerPrefetchThreadFactory;
    }

    public void setConsumerPrefetchThreadFactory(MessagingClientThreadFactory consumerPrefetchThreadFactory) {
        this.consumerPrefetchThreadFactory = Optional.ofNullable(consumerPrefetchThreadFactory).orElse(this.consumerPrefetchThreadFactory);
        ;
    }

    public ProviderConfiguration withConsumerThreadFactory(MessagingClientThreadFactory consumerThreadFactory) {
        setConsumerPrefetchThreadFactory(consumerThreadFactory);
        return this;
    }
}
