package com.chongctech.pulsar.core.container.ack;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @author gow
 * @date 2021/7/26
 */
@Slf4j
public abstract class BaseAckStrategy implements AckStrategy {

    protected Consumer<?> consumer;
    protected MessageId latestMessageId;
    private MessageId ackId;

    public void commit() {
        assert consumer != null : "consumer not init";
        if (ackId == null || ackId != latestMessageId) {
            try {
                consumer.acknowledgeCumulative(latestMessageId);
                ackId = latestMessageId;
            } catch (PulsarClientException e) {
                log.info("acknowledgeCumulative failed msgId={},e.msg={},e.cause={}", latestMessageId, e.getMessage(),
                        e.getCause());
                consumer.negativeAcknowledge(latestMessageId);
            }
        }
    }

    @Override
    public void setConsumer(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    abstract void updateMessageId(MessageId messageId);
}
