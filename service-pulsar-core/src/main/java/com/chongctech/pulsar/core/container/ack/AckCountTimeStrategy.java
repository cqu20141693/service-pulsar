package com.chongctech.pulsar.core.container.ack;

import com.chongctech.pulsar.core.domain.ContainerProperties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.MessageId;

/**
 * @author gow
 * @date 2021/7/26
 */
public class AckCountTimeStrategy extends BaseAckStrategy {

    private final ContainerProperties containerProperties;
    private int count = 0;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

    public AckCountTimeStrategy(ContainerProperties containerProperties) {
        this.containerProperties = containerProperties;

    }

    @Override
    public void processCommits(MessageId messageId) {

        updateMessageId(messageId);
        this.count++;
        boolean countExceeded = this.count >= this.containerProperties.getAckCount();
        if (countExceeded) {
            commitCumulative();
            count = 0;
        }
        startTimeAck();
    }

    private void startTimeAck() {
        if (started.compareAndSet(false, true)) {
            long ackTimeMills = containerProperties.getAckTimeMills();
            // 每隔一定时间ack messageId
            executorService
                    .scheduleWithFixedDelay(this::commitCumulative, ackTimeMills, ackTimeMills, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    void updateMessageId(MessageId messageId) {
        this.latestMessageId = messageId;
    }
}
