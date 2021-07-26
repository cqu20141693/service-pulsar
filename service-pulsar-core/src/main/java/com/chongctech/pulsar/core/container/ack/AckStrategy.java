package com.chongctech.pulsar.core.container.ack;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

/**
 * @author gow
 * @date 2021/7/26
 */
public interface AckStrategy {
    void processCommits(MessageId messageId);

    void setConsumer(Consumer<?> consumer);
}
