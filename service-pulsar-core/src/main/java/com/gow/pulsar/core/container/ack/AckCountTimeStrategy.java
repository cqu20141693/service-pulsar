package com.gow.pulsar.core.container.ack;

import com.gow.pulsar.core.domain.ContainerProperties;
import com.gow.pulsar.core.utils.PulsarLog;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
    private ScheduledFuture<?> scheduledFuture;

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

    @Override
    public void finalCommit() {
        if (started.compareAndSet(true, false)) {
            commitCumulative();
            scheduledFuture.cancel(true);
            executorService.shutdown();
        } else {
            PulsarLog.log.debug("not started or final commit has been called ");
        }
    }

    private void startTimeAck() {
        if (started.compareAndSet(false, true)) {
            long ackTimeMills = containerProperties.getAckTimeMills();
            // 每隔一定时间ack messageId
            scheduledFuture = executorService
                    .scheduleWithFixedDelay(this::commitCumulative, ackTimeMills, ackTimeMills, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    void updateMessageId(MessageId messageId) {
        this.latestMessageId = messageId;
    }
}
