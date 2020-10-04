package com.amazon.sqs.javamessaging;

import lombok.Data;

import javax.jms.MessageListener;

@Data
class CallbackEntry {

    private final MessageListener messageListener;

    private final FetchedMessage fetchedMessage;
}
