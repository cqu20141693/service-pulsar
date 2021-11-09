package com.gow.pulsar.core.producer;

import org.apache.pulsar.client.api.ProducerBuilder;

/**
 * @author gow
 * @date 2021/7/10 0010
 */
@FunctionalInterface
public interface ProducerCustomizer<T> {
    void customize(ProducerBuilder<T> producerBuilder);
}
