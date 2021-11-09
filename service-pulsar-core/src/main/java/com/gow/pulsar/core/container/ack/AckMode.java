package com.gow.pulsar.core.container.ack;

/**
 * @author gow
 * @date 2021/7/26
 */
public enum AckMode {

    /**
     * Commit after each record is processed by the listener.
     */
    RECORD,

    /**
     * Commit whatever has already been processed before the next poll.
     */
    BATCH,

    /**
     * Commit pending updates after
     * setAckTime(long) ackTime has elapsed.
     */
    TIME,

    /**
     * Commit pending updates after
     * (int) ackCount has been exceeded.
     */
    COUNT,

    /**
     * Commit pending updates after
     * setAckCount(int) ackCount has been
     * exceeded or after setAckTime(long) ackTime has elapsed.
     */
    COUNT_TIME,

    /**
     * User takes responsibility for acks using an
     */
    MANUAL,

    /**
     * User takes responsibility for acks using an
     * The consumer immediately processes the commit.
     */
    MANUAL_IMMEDIATE;
}
