package com.amazon.sqs.javamessaging;

import lombok.Builder;
import lombok.Data;

import javax.jms.Message;

@Data
@Builder
public class FetchedMessage {
    private final PrefetchManager prefetchManager;

    private final Message message;
}
