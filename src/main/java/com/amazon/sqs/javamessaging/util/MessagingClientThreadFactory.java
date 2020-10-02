package com.amazon.sqs.javamessaging.util;

import java.util.concurrent.ThreadFactory;

public interface MessagingClientThreadFactory extends ThreadFactory {
    boolean wasThreadCreatedWithThisThreadGroup(Thread thread);
}
