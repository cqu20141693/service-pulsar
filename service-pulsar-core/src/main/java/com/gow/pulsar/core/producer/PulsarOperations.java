package com.gow.pulsar.core.producer;

import com.gow.pulsar.core.domain.ProducerRecord;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;

/**
 * @author gow
 * @date 2021/7/20
 */
public interface PulsarOperations {
    <V> MessageId send(String topic, V value);

    <V> MessageId send(String topic, String key, V value);

    <V> MessageId send(ProducerRecord<V> record);

    <V> CompletableFuture<MessageId> sendAsync(ProducerRecord<V> record);
}
