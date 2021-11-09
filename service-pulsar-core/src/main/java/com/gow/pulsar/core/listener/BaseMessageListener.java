package com.gow.pulsar.core.listener;

import com.gow.pulsar.core.container.ack.AckStrategy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

/**
 * @author gow
 * @date 2021/7/20
 */
@Slf4j
public abstract class BaseMessageListener<T> implements MessageListener<T> {
    protected Method targetMethod;

    protected Object bean;
    protected final AckStrategy ackStrategy;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public BaseMessageListener(Method targetMethod, Object bean,
                               AckStrategy ackStrategy) {
        this.targetMethod = targetMethod;
        this.bean = bean;
        this.ackStrategy = ackStrategy;
    }

    @SneakyThrows
    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        if (!closed.get()) {
            if (targetMethod.getGenericParameterTypes().length == 1) {
                ackStrategy.processCommits(msg.getMessageId());
            }
            doInvoke(consumer, msg);
        } else {
            log.debug("stop invoked, consumer will ignore msg");
        }
    }

    public abstract void doInvoke(Consumer<T> consumer, Message<T> msg)
            throws IllegalAccessException, InvocationTargetException;

    @Override
    public void reachedEndOfTopic(Consumer<T> consumer) {
        MessageListener.super.reachedEndOfTopic(consumer);
    }

    public void stop() {
        closed.compareAndSet(false, true);
        ackStrategy.finalCommit();
    }
}
