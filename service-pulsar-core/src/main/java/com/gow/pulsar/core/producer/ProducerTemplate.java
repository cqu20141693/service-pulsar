package com.gow.pulsar.core.producer;

import com.gow.pulsar.core.container.PulsarContainer;
import com.gow.pulsar.core.domain.ProducerRecord;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

/**
 * @author gow
 * @date 2021/7/20
 */
@Slf4j
public class ProducerTemplate implements PulsarOperations {

    private PulsarContainer container;

    public ProducerTemplate(PulsarContainer container) {
        this.container = container;
    }

    @Override
    public <V> MessageId send(String topic, V value) {
        ProducerRecord<V> record = new ProducerRecord<>(topic, value);
        return send(record);
    }

    @Override
    public <V> MessageId send(String topic, String key, V value) {
        ProducerRecord<V> record = new ProducerRecord<>(topic, key, value);
        return send(record);
    }

    @Override
    public <V> MessageId send(ProducerRecord<V> record) {
        try {
            Producer<V> producer = (Producer<V>) container.getProducer(record.topic());
            if (producer == null) {
                log.warn("producer not exist for topic={}", record.topic());
                return null;
            }
            String key = record.key();
            V value = record.value();
            return producer.newMessage().key(key).value(value).send();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <V> CompletableFuture<MessageId> sendAsync(ProducerRecord<V> record) {
        try {
            Producer<V> producer = (Producer<V>) container.getProducer(record.topic());
            if (producer == null) {
                log.warn("producer not exist for topic={}", record.topic());
                return null;
            }

            return producer.newMessage().key(record.key()).value(record.value()).sendAsync();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
