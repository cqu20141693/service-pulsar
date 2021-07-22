package com.chongctech.pulsar.core.domain;


import static org.apache.pulsar.client.api.ConsumerCryptoFailureAction.FAIL;
import static org.apache.pulsar.client.api.MessageId.earliest;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author gow
 * @date 2021/7/2
 */
@Configuration
@ConfigurationProperties("com.chongctech.service.pulsar")
@Data
public class PulsarProperties {
    private String cluster = "dev";
    @NotNull
    private String serviceUrl;
    @NotNull
    private ClientProperties client;

    private ProducerProperties producer = new ProducerProperties();
    private ConsumerProperties consumer = new ConsumerProperties();
    private ReaderProperties reader;
    private AdminProperties admin;

    /**
     * @author gow
     * @date 2021/7/19
     */
    @Data
    public static class ClientProperties {
        private String role;
        @NotNull
        private String jwtToken;
        private String authPluginClassName = null;
        private String authParams = null;
        private Long operationTimeoutMs = 30000L;
        private Long statsIntervalSeconds = 60L;
        private Integer numIoThreads = 1;
        private Integer numListenerThreads = 1;
        private Boolean useTcpNoDelay = true;
        private Boolean useTls = false;
        private String tlsTrustCertsFilePath = null;
        private Boolean tlsAllowInsecureConnection = false;
        private Boolean tlsHostnameVerificationEnable = false;
        private Integer concurrentLookupRequest = 5000;
        private Integer maxLookupRequest = 50000;
        private Integer maxNumberOfRejectedRequestPerConnection = 50;
        private Integer keepAliveIntervalSeconds = 30;
        private Integer connectionTimeoutMs = 10000;
        private Integer requestTimeoutMs = 60000;
        private Integer defaultBackoffIntervalNanos;
        private Long maxBackoffIntervalNanos;

        public ClientProperties() {
            this.defaultBackoffIntervalNanos = Math.toIntExact(TimeUnit.MILLISECONDS.toNanos(100L));
            this.maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(30L);
        }

    }

    /**
     * Super user, used to configure element data
     *
     * @author gow
     * @date 2021/7/5
     */
    @Data
    public static class AdminProperties {
        private String webServiceUrl;
        private String adminToken;
        private String authPluginClassName;
        private String authParams;
        private Boolean useTsl = false;
        private Boolean tlsAllowInsecureConnection = false;
        private String tlsTrustCertsFilePath;
    }

    /**
     * @author gow
     * @date 2021/7/2
     */
    @Data
    public static class ReaderProperties {

        private Set<String> topicNames = new HashSet();
        @JsonIgnore
        private MessageId startMessageId = earliest;
        @JsonIgnore
        private long startMessageFromRollbackDurationInSec = 1;
        private int receiverQueueSize = 1000;
        private String readerName = null;
        private String subscriptionRolePrefix = null;
        private String subscriptionName = null;
        private CryptoKeyReader cryptoKeyReader = null;
        private ConsumerCryptoFailureAction cryptoFailureAction = FAIL;
        private boolean readCompacted = false;
    }

    /**
     * @author gow
     * @date 2021/7/2
     */
    @Data
    public static class ConsumerProperties {
        private SubscriptionMode subscriptionMode;
        private SubscriptionInitialPosition subscriptionInitialPosition;
        private RegexSubscriptionMode regexSubscriptionMode;
        private Integer receiverQueueSize;
        private Long acknowledgementsGroupTimeMicros;
        private Long negativeAckRedeliveryDelayMicros;
        private Integer maxTotalReceiverQueueSizeAcrossPartitions;
        private String consumerName;
        private Long ackTimeoutMillis;
        private Long tickDurationMillis;
        private Integer priorityLevel;
        private ConsumerCryptoFailureAction cryptoFailureAction;
        private SortedMap<String, String> properties;
        private Boolean readCompacted;
        private Integer patternAutoDiscoveryPeriod;
        private DeadLetterPolicy deadLetterPolicy;
        private Boolean autoUpdatePartitions;
        private Boolean replicateSubscriptionState;
        private Integer maxPendingChunkedMessage;
        private Boolean autoAckOldestChunkedMessageOnQueueFull;
        private Long expireTimeOfIncompleteChunkedMessageMillis;
        private Boolean enableRetry;

        public ConsumerProperties() {
            this.subscriptionMode = SubscriptionMode.Durable;
            this.subscriptionInitialPosition = SubscriptionInitialPosition.Latest;
            this.regexSubscriptionMode = RegexSubscriptionMode.PersistentOnly;
            this.receiverQueueSize = 1000;
            this.acknowledgementsGroupTimeMicros = TimeUnit.MILLISECONDS.toMicros(100L);
            this.negativeAckRedeliveryDelayMicros = TimeUnit.MINUTES.toMicros(1L);
            this.maxTotalReceiverQueueSizeAcrossPartitions = 50000;
            this.consumerName = null;
            this.ackTimeoutMillis = 0L;
            this.tickDurationMillis = 1000L;
            this.priorityLevel = 0;
            this.cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;
            this.properties = new TreeMap<>();
            this.readCompacted = false;
            this.patternAutoDiscoveryPeriod = 1;
            this.deadLetterPolicy = null;
            this.autoUpdatePartitions = true;
            this.replicateSubscriptionState = false;
            this.maxPendingChunkedMessage = 1000;
            this.autoAckOldestChunkedMessageOnQueueFull = true;
            this.expireTimeOfIncompleteChunkedMessageMillis = 3600000L;
            this.enableRetry = false;
        }

    }

    /**
     * @author gow
     * @date 2021/7/21
     *
     * topic info
     */
    @Data
    @EqualsAndHashCode
    public static class TopicInfo {
        private String name;
        private PulsarSchemaType schema = PulsarSchemaType.String;
        private Class<?> jsonClass;
    }

    /**
     * @author gow
     * @date 2021/7/2
     */
    @Data
    public static class ProducerProperties {
        private Set<PulsarProperties.TopicInfo> topics = new HashSet<>();
        private String producerName;
        private Integer sendTimeoutMs = 30000;
        private Boolean blockIfQueueFull = false;
        private Integer maxPendingMessages = 1000;
        private Integer maxPendingMessagesAcrossPartitions = 50000;
        private MessageRoutingMode messageRoutingMode;
        private HashingScheme hashingScheme;
        private ProducerCryptoFailureAction cryptoFailureAction;
        private Long batchingMaxPublishDelayMicros;
        private Integer batchingMaxMessages;
        private Boolean batchingEnabled;
        private BatcherBuilder batcherBuilder;
        private CompressionType compressionType;
        private Integer batchMaxBytes;
        private ProducerAccessMode accessMode;
        private Boolean chunkingEnabled;

        public ProducerProperties() {
            this.messageRoutingMode = MessageRoutingMode.RoundRobinPartition;
            this.hashingScheme = HashingScheme.JavaStringHash;
            this.cryptoFailureAction = ProducerCryptoFailureAction.FAIL;
            this.batchingMaxPublishDelayMicros = TimeUnit.MILLISECONDS.toMicros(1L);
            this.batchingMaxMessages = 1000;
            this.batchingEnabled = false;
            this.batcherBuilder = BatcherBuilder.DEFAULT;
            this.compressionType = CompressionType.NONE;
            this.batchMaxBytes = 83886080;
            this.accessMode = ProducerAccessMode.Shared;
            this.chunkingEnabled = false;
        }

    }

}
