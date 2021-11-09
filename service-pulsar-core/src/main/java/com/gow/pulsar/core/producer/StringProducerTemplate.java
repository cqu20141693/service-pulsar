package com.gow.pulsar.core.producer;

import com.gow.pulsar.core.container.PulsarContainer;
import com.gow.pulsar.core.domain.ProducerRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.springframework.beans.factory.DisposableBean;

/**
 * @author gow
 * @date 2021/7/20
 */
@Slf4j
public class StringProducerTemplate implements DisposableBean {

    private PulsarContainer container;

    public StringProducerTemplate(PulsarContainer container) {
        this.container = container;
    }


    public MessageId send(String topic, String value) throws PulsarClientException {
        ProducerRecord<String> record = new ProducerRecord<>(topic, value);
        return send(record);
    }


    public MessageId send(String topic, String key, String value) throws PulsarClientException {
        ProducerRecord<String> record = new ProducerRecord<>(topic, key, value);
        return send(record);
    }


    public MessageId send(ProducerRecord<String> record) throws PulsarClientException {

        Producer<String> producer = getProducer(record);
        if (producer == null) {
            return null;
        }
        String value = record.value();
        TypedMessageBuilder<String> builder = producer.newMessage();
        Optional.ofNullable(record.key()).ifPresent(builder::key);
        return builder.value(value).send();

    }

    private Producer<String> getProducer(ProducerRecord<String> record) {
        Producer<String> producer = container.getStringProducer(record.topic());
        if (producer == null) {
            log.warn("producer not exist for topic={}", record.topic());
            return null;
        }
        if (!producer.isConnected()) {
            log.warn("producer is disconnected topic={}", record.topic());
            return null;
        }
        return producer;
    }

    public MessageId sendAfter(String topic, String value, long time, TimeUnit unit) throws PulsarClientException {
        ProducerRecord<String> record = new ProducerRecord<>(topic, value);
        return sendAfter(record, time, unit);
    }


    public MessageId sendAfter(String topic, String key, String value, long time, TimeUnit unit)
            throws PulsarClientException {
        ProducerRecord<String> record = new ProducerRecord<>(topic, key, value);
        return sendAfter(record, time, unit);
    }

    public MessageId sendAfter(ProducerRecord<String> record, long time, TimeUnit unit) throws PulsarClientException {

        Producer<String> producer = getProducer(record);
        if (producer == null) {
            return null;
        }
        String value = record.value();
        TypedMessageBuilder<String> builder = producer.newMessage();
        Optional.ofNullable(record.key()).ifPresent(builder::key);
        return builder.deliverAfter(time, unit).value(value).send();
    }

    public MessageId sendAt(String topic, String value, long timestamp) throws PulsarClientException {
        ProducerRecord<String> record = new ProducerRecord<>(topic, value);
        return sendAt(record, timestamp);
    }


    public MessageId sendAt(String topic, String key, String value, long timestamp) throws PulsarClientException {
        ProducerRecord<String> record = new ProducerRecord<>(topic, key, value);
        return sendAt(record, timestamp);
    }

    public MessageId sendAt(ProducerRecord<String> record, long timestamp) throws PulsarClientException {

        Producer<String> producer = getProducer(record);
        if (producer == null) {
            return null;
        }
        String value = record.value();
        TypedMessageBuilder<String> builder = producer.newMessage();
        Optional.ofNullable(record.key()).ifPresent(builder::key);
        return builder.deliverAt(timestamp).value(value).send();

    }


    public CompletableFuture<MessageId> sendAsync(String topic, String value) {
        ProducerRecord<String> record = new ProducerRecord<>(topic, value);
        return sendAsync(record);
    }


    public CompletableFuture<MessageId> sendAsync(String topic, String key, String value) {
        ProducerRecord<String> record = new ProducerRecord<>(topic, key, value);
        return sendAsync(record);
    }

    public CompletableFuture<MessageId> sendAsync(ProducerRecord<String> record) {
        Producer<String> producer = getProducer(record);
        if (producer == null) {
            return null;
        }
        TypedMessageBuilder<String> builder = producer.newMessage();
        Optional.ofNullable(record.key()).ifPresent(builder::key);
        return builder.value(record.value()).sendAsync();
    }

    @Override
    public void destroy() throws Exception {
        container.getProducerMap().forEach((s, producer) -> {
            producer.closeAsync();
        });
    }
}
