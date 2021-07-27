package com.chongctech.pulsar.core.container;

import com.chongctech.pulsar.core.annotation.SubscribeHolder;
import com.chongctech.pulsar.core.domain.ContainerProperties;
import com.chongctech.pulsar.core.domain.PulsarProperties;
import com.chongctech.pulsar.core.domain.PulsarSchemaType;
import com.chongctech.pulsar.core.event.PulsarContainerStopEvent;
import com.chongctech.pulsar.core.factory.PulsarFactory;
import com.chongctech.pulsar.core.listener.SubscribeMessageListener;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;

/**
 * @author gow
 * @date 2021/7/19 0019
 *
 * container should provide client for the creation of producers and consumers, and manage them
 * At the same time, it needs to be able to provide custom producer and consumer attributes
 */
@Slf4j
public class PulsarContainer implements SmartInitializingSingleton, SmartLifecycle, ApplicationEventPublisherAware {

    private final PulsarProperties properties;

    private final PulsarFactory pulsarFactory;

    private final PulsarClient client;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ApplicationEventPublisher applicationEventPublisher;

    private ContainerProperties containerProperties = new ContainerProperties();
    private Map<Consumer<?>, SubscribeMessageListener> consumers = new HashMap<>();
    private Map<String, Producer<?>> producerMap = new HashMap<>();
    private Map<String, Producer<String>> stringProducerMap = new HashMap<>();

    public PulsarContainer(PulsarProperties properties, PulsarFactory pulsarFactory, PulsarClient client) {
        this.properties = properties;
        this.pulsarFactory = pulsarFactory;
        this.client = client;
    }

    public Producer<?> getProducer(String topic) {
        return producerMap.get(topic);
    }

    public ContainerProperties containerProperties() {
        return this.containerProperties;
    }

    public Producer<String> getStringProducer(String topic) {
        Producer<String> stringProducer = stringProducerMap.get(topic);
        if (stringProducer == null) {
            createStringProducer(properties.getProducer(), topic);
        }
        return stringProducerMap.get(topic);
    }

    public Map<String, Producer<?>> getProducerMap() {
        return producerMap;
    }

    public <T> void addConsumer(SubscribeHolder holder,
                                Method handlerMethod, Object bean) {
        Schema<?> schema = getSchema(holder.getSchema(), holder.getJsonClass());

        ConsumerBuilder<?> builder =
                pulsarFactory.consumerBuilder(client, holder.getTopic(), holder.getPattern(), schema,
                        properties.getConsumer());
        builder.subscriptionName(holder.getRealSubscribeName());
        builder.subscriptionType(holder.getSubscriptionType());

        SubscribeMessageListener messageListener =
                new SubscribeMessageListener<>(handlerMethod, bean, containerProperties);
        builder.messageListener(messageListener);
        try {
            Consumer<?> consumer = builder.subscribe();
            consumers.put(consumer, messageListener);
        } catch (PulsarClientException e) {
            e.printStackTrace();
            log.warn("subscribe failed, topic={},subscriptionName={},subscriptionType={}", holder.getTopic(),
                    holder.getRealSubscribeName(), holder.getSubscriptionType());
        }
    }

    private Schema<?> getSchema(PulsarSchemaType schemaType, Class<?> aClass) {
        Schema<?> schema;
        if (schemaType == PulsarSchemaType.Json) {
            schema = Schema.JSON(aClass);
        } else {
            schema = schemaType.getSchema();
        }
        return schema;
    }

    @Override
    public void afterSingletonsInstantiated() {
        PulsarProperties.ProducerProperties producerProperties = properties.getProducer();
        producerProperties.getTopics().forEach(topic
                -> {
            Schema<?> schema = getSchema(topic.getSchema(), topic.getJsonClass());
            createProducer(producerProperties, topic.getName(), schema);
        });
    }

    private void createProducer(PulsarProperties.ProducerProperties producerProperties, String topic,
                                Schema<?> schema) {

        if (schema == Schema.STRING) {
            createStringProducer(producerProperties, topic);
        } else {
            ProducerBuilder<?> builder = pulsarFactory.producerBuilder(client, topic, schema, producerProperties);
            try {
                Producer<?> producer = builder.create();
                producerMap.put(topic, producer);
            } catch (PulsarClientException e) {
                e.printStackTrace();
                log.warn("subscribe failed, topic={},producerName={}", topic, producerProperties.getProducerName());
            }
        }
    }

    private void createStringProducer(PulsarProperties.ProducerProperties producerProperties, String topic) {
        ProducerBuilder<String> stringProducerBuilder =
                pulsarFactory.producerBuilder(client, topic, Schema.STRING, producerProperties);
        try {
            Producer<String> producer = stringProducerBuilder.create();
            stringProducerMap.put(topic, producer);
            producerMap.put(topic, producer);
        } catch (PulsarClientException e) {
            e.printStackTrace();
            log.warn("subscribe failed, topic={},producerName={}", topic, producerProperties.getProducerName());
        }
    }

    @Override
    public void start() {
        if (this.running.compareAndSet(false, true)) {
            log.debug("container start,monitor can start");
        } else {
            log.info("container started !");
        }

    }

    @Override
    public void stop() {
        if (this.running.compareAndSet(true, false)) {
            log.info("container stopping ");
            consumers.forEach((consumer, listener) -> {
                listener.stop();
                consumer.closeAsync();
            });
            applicationEventPublisher.publishEvent(new PulsarContainerStopEvent(this));
        }
    }

    @Override
    public void stop(Runnable callback) {
        this.stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }
}
