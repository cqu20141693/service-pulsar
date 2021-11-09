package com.gow.pulsar.core.route;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

/**
 * @author gow
 * @date 2021/7/6 0006
 *
 * when messageRoutingMode custom can config producer messageRouter
 */
public class FixedPartitionRouter implements MessageRouter {
    private final int partitionNum;

    public FixedPartitionRouter(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public int choosePartition(Message msg, TopicMetadata metadata) {
        return partitionNum;
    }
}
