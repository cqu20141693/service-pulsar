package com.chongctech.pulsar.core.factory;

import com.chongctech.pulsar.core.domain.PulsarProperties;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;

/**
 * @author gow
 * @date 2021/7/2
 */
@Slf4j
public class PulsarFactory {

    public PulsarClient createClient(String serviceUrl, PulsarProperties.ClientProperties clientProperties)
            throws PulsarClientException {
        ClientBuilder builder = PulsarClient.builder()
                .serviceUrl(serviceUrl);
        configClient(builder, clientProperties);
        log.info("create  client serviceUrl={}", serviceUrl);
        return builder.build();
    }

    private void configClient(ClientBuilder builder,
                              PulsarProperties.ClientProperties clientProperties) {
        Optional.ofNullable(clientProperties).ifPresent(properties -> {
            Optional.ofNullable(properties.getJwtToken()).ifPresent(t -> {
                builder.authentication(AuthenticationFactory.token(t));
            });
            builder.ioThreads(properties.getNumIoThreads());
            builder.listenerThreads(properties.getNumListenerThreads());
        });
    }

    public <T> ProducerBuilder<T> producerBuilder(PulsarClient pulsarClient, String topic, Schema<T> schema,
                                                  PulsarProperties.ProducerProperties properties) {
        ProducerBuilder<T> producerBuilder = pulsarClient.newProducer(schema).topic(topic);
        return configProducer(producerBuilder, properties);
    }

    public <T> Producer<T> newProducer(PulsarClient pulsarClient, String topic, Schema<T> schema,
                                       PulsarProperties.ProducerProperties properties)
            throws PulsarClientException {
        return producerBuilder(pulsarClient, topic, schema, properties).create();
    }


    public <T> CompletableFuture<Producer<T>> newAsyncProducer(PulsarClient pulsarClient, String topic,
                                                               Schema<T> schema,
                                                               PulsarProperties.ProducerProperties properties) {
        return producerBuilder(pulsarClient, topic, schema, properties).createAsync();
    }

    private <T> ProducerBuilder<T> configProducer(ProducerBuilder<T> producerBuilder,
                                                  PulsarProperties.ProducerProperties properties) {
        return Optional.ofNullable(properties).map(producerProperties -> {
            Optional.ofNullable(properties.getProducerName()).ifPresent(producerBuilder::producerName);
            return producerBuilder.sendTimeout(properties.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
                    .blockIfQueueFull(properties.getBlockIfQueueFull())
                    .maxPendingMessages(properties.getMaxPendingMessages())
                    .maxPendingMessagesAcrossPartitions(properties.getMaxPendingMessagesAcrossPartitions())
                    .messageRoutingMode(properties.getMessageRoutingMode())
                    .hashingScheme(properties.getHashingScheme())
                    .cryptoFailureAction(properties.getCryptoFailureAction())
                    .batchingMaxPublishDelay(properties.getBatchingMaxPublishDelayMicros(), TimeUnit.MICROSECONDS)
                    .batchingMaxMessages(properties.getBatchingMaxMessages())
                    .batcherBuilder(properties.getBatcherBuilder())
                    .enableBatching(properties.getBatchingEnabled())
                    .batchingMaxBytes(properties.getBatchMaxBytes())
                    .compressionType(properties.getCompressionType())
                    .accessMode(properties.getAccessMode())
                    .enableChunking(properties.getChunkingEnabled());
        }).orElse(producerBuilder);

    }

    public <T> ConsumerBuilder<T> consumerBuilder(PulsarClient pulsarClient, String topic, boolean pattern,
                                                  Schema<T> schema,
                                                  PulsarProperties.ConsumerProperties properties) {
        ConsumerBuilder<T> consumerBuilder;
        if (!pattern) {
            consumerBuilder = pulsarClient.newConsumer(schema).topic(topic);
        } else {
            consumerBuilder = pulsarClient.newConsumer(schema).topicsPattern(topic);
        }
        return configConsumer(consumerBuilder, properties);
    }

    public <T> Consumer<T> newConsumer(PulsarClient pulsarClient, String topic, boolean pattern, Schema<T> schema,
                                       PulsarProperties.ConsumerProperties properties)
            throws PulsarClientException {
        return consumerBuilder(pulsarClient, topic, pattern, schema, properties).subscribe();
    }

    public <T> CompletableFuture<Consumer<T>> newAsyncConsumer(PulsarClient pulsarClient, String topic, boolean pattern,
                                                               Schema<T> schema,
                                                               PulsarProperties.ConsumerProperties properties) {
        return consumerBuilder(pulsarClient, topic, pattern, schema, properties).subscribeAsync();
    }

    private <T> ConsumerBuilder<T> configConsumer(ConsumerBuilder<T> consumerBuilder,
                                                  PulsarProperties.ConsumerProperties consumer) {
        return Optional.ofNullable(consumer).map(consumerProperties -> {
            Optional.ofNullable(consumer.getConsumerName()).ifPresent(consumerBuilder::consumerName);
            Optional.ofNullable(consumer.getSubscriptionMode()).ifPresent(consumerBuilder::subscriptionMode);
            Optional.ofNullable(consumer.getDeadLetterPolicy()).ifPresent(consumerBuilder::deadLetterPolicy);
            if (consumer.getProperties().size() > 0) {
                consumerBuilder.properties(consumer.getProperties());
            }
            return consumerBuilder.receiverQueueSize(consumer.getReceiverQueueSize())
                    .acknowledgmentGroupTime(consumer.getAcknowledgementsGroupTimeMicros(), TimeUnit.MICROSECONDS)
                    .enableBatchIndexAcknowledgment(consumer.isBatchIndexAcknowledgmentEnabled())
                    .negativeAckRedeliveryDelay(consumer.getNegativeAckRedeliveryDelayMicros(), TimeUnit.MICROSECONDS)
                    .maxTotalReceiverQueueSizeAcrossPartitions(consumer.getMaxTotalReceiverQueueSizeAcrossPartitions())
                    .ackTimeout(consumer.getAckTimeoutMillis(), TimeUnit.MILLISECONDS)
                    .ackTimeoutTickTime(consumer.getTickDurationMillis(), TimeUnit.MILLISECONDS)
                    .priorityLevel(consumer.getPriorityLevel())
                    .cryptoFailureAction(consumer.getCryptoFailureAction())
                    .readCompacted(consumer.getReadCompacted())
                    .subscriptionInitialPosition(consumer.getSubscriptionInitialPosition())
                    .patternAutoDiscoveryPeriod(consumer.getPatternAutoDiscoveryPeriod())
                    .subscriptionTopicsMode(consumer.getRegexSubscriptionMode())
                    .autoUpdatePartitions(consumer.getAutoUpdatePartitions())
                    .replicateSubscriptionState(consumer.getReplicateSubscriptionState())
                    .maxPendingChunkedMessage(consumer.getMaxPendingChunkedMessage())
                    .autoAckOldestChunkedMessageOnQueueFull(consumer.getAutoAckOldestChunkedMessageOnQueueFull())
                    .expireTimeOfIncompleteChunkedMessage(consumer.getExpireTimeOfIncompleteChunkedMessageMillis(),
                            TimeUnit.MILLISECONDS)
                    .enableRetry(consumer.getEnableRetry());
        }).orElse(consumerBuilder);
    }

    public Reader<String> newReader(PulsarClient pulsarClient, String topic, PulsarProperties.ReaderProperties reader)
            throws PulsarClientException {
        ReaderBuilder<String> readerBuilder = pulsarClient.newReader(Schema.STRING)
                .topic(topic);
        return configReader(readerBuilder, reader).create();
    }

    private ReaderBuilder<String> configReader(ReaderBuilder<String> readerBuilder,
                                               PulsarProperties.ReaderProperties reader) {
        Optional.ofNullable(reader.getCryptoKeyReader()).ifPresent(readerBuilder::cryptoKeyReader);
        Optional.ofNullable(reader.getReaderName()).ifPresent(readerBuilder::readerName);
        Optional.ofNullable(reader.getSubscriptionName()).ifPresent(readerBuilder::subscriptionName);
        Optional.ofNullable(reader.getSubscriptionRolePrefix()).ifPresent(readerBuilder::subscriptionRolePrefix);
        return readerBuilder.readCompacted(reader.isReadCompacted())
                .cryptoFailureAction(reader.getCryptoFailureAction())
                .startMessageId(reader.getStartMessageId())
                .receiverQueueSize(reader.getReceiverQueueSize())
                .startMessageFromRollbackDuration(reader.getStartMessageFromRollbackDurationInSec(), TimeUnit.SECONDS);
    }

}
