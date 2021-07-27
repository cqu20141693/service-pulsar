package com.chongctech.pulsar.core.listener;

import com.chongctech.pulsar.core.container.ack.AckCountTimeStrategy;
import com.chongctech.pulsar.core.container.ack.AckMode;
import com.chongctech.pulsar.core.container.ack.AckStrategy;
import com.chongctech.pulsar.core.container.ack.DefaultAckStrategy;
import com.chongctech.pulsar.core.domain.ContainerProperties;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
public class SubscribeMessageListener<T> implements MessageListener<T> {
    private Method targetMethod;

    private Object bean;
    private final AckStrategy ackStrategy;
    private AtomicBoolean consumerSet = new AtomicBoolean(false);
    private AtomicBoolean closed = new AtomicBoolean(false);

    public SubscribeMessageListener(Method targetMethod, Object bean,
                                    ContainerProperties containerProperties) {
        this.targetMethod = targetMethod;
        this.bean = bean;
        AckMode ackMode = containerProperties.getAckMode();
        if (ackMode == AckMode.COUNT_TIME) {
            ackStrategy = new AckCountTimeStrategy(containerProperties);
        } else {
            ackStrategy = new DefaultAckStrategy();
        }
    }

    @SneakyThrows
    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {
        if (!closed.get()) {
            consumerInit(consumer);
            ackStrategy.processCommits(msg.getMessageId());
            Type[] genericParameterTypes = targetMethod.getGenericParameterTypes();
            targetMethod.setAccessible(true);
            if (genericParameterTypes.length == 2) {
                targetMethod.invoke(bean, consumer, msg);
            } else if (genericParameterTypes.length == 1) {

                if (genericParameterTypes[0] instanceof ParameterizedType) {
                    ParameterizedType genericParameterType = (ParameterizedType) genericParameterTypes[0];
                    if (genericParameterType.getRawType() == Message.class) {
                        targetMethod.invoke(bean, msg);
                    } else {
                        log.warn("pulsar subscribe bean={} targetMethod={} parameter error", bean.getClass(),
                                targetMethod.getName());
                        throw new RuntimeException("parameter error");
                    }
                } else {
                    targetMethod.invoke(bean, msg.getValue());
                }
            } else {
                log.error("bean={} targetMethod={} parameter error", bean.getClass(), targetMethod.getName());
            }
        } else {
            log.debug("SubscribeMessageListener is closed, consumer will ignore msg");
        }
    }

    private void consumerInit(Consumer<T> consumer) {
        if (consumerSet.compareAndSet(false, true)) {
            ackStrategy.setConsumer(consumer);
        }
    }

    public void stop() {
        closed.compareAndSet(false, true);
        ackStrategy.finalCommit();
    }
}
