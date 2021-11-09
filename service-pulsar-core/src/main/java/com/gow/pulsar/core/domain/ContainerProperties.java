package com.gow.pulsar.core.domain;

import com.gow.pulsar.core.container.ack.AckMode;
import lombok.Data;

/**
 * @author gow
 * @date 2021/7/26
 */
@Data
public class ContainerProperties {
    private AckMode ackMode = AckMode.COUNT_TIME;
    private int ackCount = 100;
    private long ackTimeMills = 100;
    private boolean commitSync = false;
}
