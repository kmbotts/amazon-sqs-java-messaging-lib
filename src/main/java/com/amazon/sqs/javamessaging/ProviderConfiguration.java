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

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ProviderConfiguration {
    public static final ProviderConfiguration DEFAULT = ProviderConfiguration.builder().build();

    @Builder.Default
    private final int numberOfMessagesToPrefetch = SQSMessagingClientConstants.MIN_BATCH;

    @Builder.Default
    private final MessagingClientThreadFactory sessionThreadFactory = SQSSession.SESSION_THREAD_FACTORY;

    @Builder.Default
    private final MessagingClientThreadFactory consumerPrefetchThreadFactory = SQSSession.CONSUMER_PREFETCH_THREAD_FACTORY;
}
