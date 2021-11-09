package com.gow.pulsar.core.domain;

import static org.apache.pulsar.client.api.Schema.BOOL;
import static org.apache.pulsar.client.api.Schema.BYTEBUFFER;
import static org.apache.pulsar.client.api.Schema.BYTES;
import static org.apache.pulsar.client.api.Schema.DATE;
import static org.apache.pulsar.client.api.Schema.DOUBLE;
import static org.apache.pulsar.client.api.Schema.FLOAT;
import static org.apache.pulsar.client.api.Schema.INSTANT;
import static org.apache.pulsar.client.api.Schema.INT16;
import static org.apache.pulsar.client.api.Schema.INT32;
import static org.apache.pulsar.client.api.Schema.INT64;
import static org.apache.pulsar.client.api.Schema.INT8;
import static org.apache.pulsar.client.api.Schema.LOCAL_DATE;
import static org.apache.pulsar.client.api.Schema.LOCAL_DATE_TIME;
import static org.apache.pulsar.client.api.Schema.LOCAL_TIME;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.apache.pulsar.client.api.Schema.TIME;
import static org.apache.pulsar.client.api.Schema.TIMESTAMP;
import org.apache.pulsar.client.api.Schema;

/**
 * @author gow
 * @date 2021/7/19 0019
 */
public enum PulsarSchemaType {
    Bytes(BYTES),
    Bytebuffer(BYTEBUFFER),
    String(STRING),
    Int8(INT8),
    Int16(INT16),
    In32(INT32),
    In64(INT64),
    Bool(BOOL),
    Float(FLOAT),
    Double(DOUBLE),
    Date(DATE),
    Time(TIME),
    Timestamp(TIMESTAMP),
    Instant(INSTANT),
    LocalDate(LOCAL_DATE),
    LocalTime(LOCAL_TIME),
    LocalDateTime(LOCAL_DATE_TIME),
    Json(null),
    Avro(null),
    KeyValue(null);;

    PulsarSchemaType(Schema<?> schema) {
        this.schema = schema;
    }

    private Schema<?> schema;

    public Schema<?> getSchema() {
        return schema;
    }


}
