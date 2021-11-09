package com.gow.pulsar.core.domain;

import java.util.Objects;

/**
 * @author gow
 * @date 2021/7/20
 */
public class ProducerRecord<V> {
    private final String topic;
    private final String key;
    private final V value;
    private final Long timestamp;


    public ProducerRecord(String topic, Integer partition, Long timestamp, String key, V value) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null.");
        } else if (timestamp != null && timestamp < 0L) {
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.",
                            timestamp));
        } else if (partition != null && partition < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.",
                            partition));
        } else {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public ProducerRecord(String topic, Integer partition, String key, V value) {
        this(topic, partition, (Long) null, key, value);
    }


    public ProducerRecord(String topic, String key, V value) {
        this(topic, (Integer) null, (Long) null, key, value);
    }

    public ProducerRecord(String topic, V value) {
        this(topic, (Integer) null, (Long) null, null, value);
    }

    public String topic() {
        return this.topic;
    }


    public String key() {
        return this.key;
    }

    public V value() {
        return this.value;
    }

    public Long timestamp() {
        return this.timestamp;
    }


    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerRecord(topic=" + this.topic + ", key=" + key + ", value="
                + value + ", timestamp=" + timestamp + ")";
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof ProducerRecord)) {
            return false;
        } else {
            ProducerRecord<?> that = (ProducerRecord) o;
            return Objects.equals(this.key, that.key) && Objects
                    .equals(this.topic, that.topic) && Objects.equals(this.value, that.value) && Objects
                    .equals(this.timestamp, that.timestamp);
        }
    }

    public int hashCode() {
        int result = this.topic != null ? this.topic.hashCode() : 0;
        result = 31 * result + (this.key != null ? this.key.hashCode() : 0);
        result = 31 * result + (this.value != null ? this.value.hashCode() : 0);
        result = 31 * result + (this.timestamp != null ? this.timestamp.hashCode() : 0);
        return result;
    }
}
