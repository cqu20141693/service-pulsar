package com.chongctech.pulsar.core.container.ack;

import com.chongctech.pulsar.core.utils.PulsarLog;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @author gow
 * @date 2021/7/26
 */
public abstract class BaseAckStrategy implements AckStrategy {

    protected Consumer<?> consumer;
    protected MessageId latestMessageId;
    private MessageId ackId;

    public void commitCumulative() {
        assert consumer != null : "consumer not init";
        if (latestMessageId != null && (ackId == null || ackId != latestMessageId) && consumer.isConnected()) {
            try {
                consumer.acknowledgeCumulative(latestMessageId);
                ackId = latestMessageId;
            } catch (PulsarClientException e) {
                PulsarLog.log.info("acknowledgeCumulative failed msgId={},e.msg={},e.cause={}", latestMessageId,
                        e.getMessage(),
                        e.getCause());
                consumer.negativeAcknowledge(latestMessageId);
            }
        } else {
            if (latestMessageId == null) {
                PulsarLog.log.debug("commitCumulative condition failed, msgId=null");
            } else if (!consumer.isConnected()) {
                PulsarLog.log.debug("commitCumulative condition failed, consumer disconnected");
            }
        }
    }

    public void commitIndividual() {
        assert consumer != null : "consumer not init";

        try {
            if (consumer.isConnected()) {
                consumer.acknowledge(latestMessageId);
            }
        } catch (PulsarClientException e) {
            PulsarLog.log.info("commitIndividual failed msgId={},e.msg={},e.cause={}", latestMessageId, e.getMessage(),
                    e.getCause());
            consumer.negativeAcknowledge(latestMessageId);
        }

    }

    @Override
    public void setConsumer(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    abstract void updateMessageId(MessageId messageId);
}
