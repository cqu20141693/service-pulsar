package com.chongctech.pulsar.core.domain;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author gow
 * @date 2021/7/7
 */
@Configuration
@ConfigurationProperties("gow.pulsar.tenant")
public class PulsarTenantUtil {
    public final static String PATH_DELIMITER = "/";
    public final static String TOPIC_DELIMITER = "-";

    public static final String persistentPrefix = "persistent:/";
    public static final String nonPersistentPrefix = "non-persistent:/";
    private final String partitionedPrefix = "p";
    private final String singlePartitionPrefix = "s";


    private String role = "gow";
    private String name = "gow";
    private String namespace = "persistent";
    private String topic = "chongC-test";


    public void setName(String name) {
        this.name = name;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setRole(String role) {
        this.role = role;
    }


    public String getRole() {
        return role;
    }

    public String getName() {
        return name;
    }

    public String getTopic() {
        return topic;
    }

    public String getOriginNamespace() {
        return namespace;
    }

    public String getNamespace() {
        return String.join(PATH_DELIMITER, name, namespace);
    }

    public String getPersistentPrefix() {
        return persistentPrefix;
    }

    public String getNonPersistentPrefix() {
        return nonPersistentPrefix;
    }

    public String getPartitionedPrefix() {
        return partitionedPrefix;
    }

    public String getSinglePartitionPrefix() {
        return singlePartitionPrefix;
    }


    public String getDefaultTopic(Boolean persistent, Boolean partitioned) {
        return getTopicName(persistent, partitioned, this.topic);
    }

    public String getTopicName(Boolean persistent, Boolean partitioned, String topic) {
        String persistentPrefixName = persistentPrefix;
        String partitionPrefixName = partitionedPrefix;
        if (!persistent) {
            persistentPrefixName = nonPersistentPrefix;
        }
        if (!partitioned) {
            partitionPrefixName = singlePartitionPrefix;
        }
        return String.join(PATH_DELIMITER, persistentPrefixName, name, namespace,
                String.join(TOPIC_DELIMITER, partitionPrefixName, topic));
    }
}