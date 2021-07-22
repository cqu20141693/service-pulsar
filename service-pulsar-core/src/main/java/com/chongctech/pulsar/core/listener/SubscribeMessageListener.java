package com.chongctech.pulsar.core.listener;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
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

    public SubscribeMessageListener(Method targetMethod, Object bean) {
        this.targetMethod = targetMethod;
        this.bean = bean;
    }

    @SneakyThrows
    @Override
    public void received(Consumer<T> consumer, Message<T> msg) {

        Type[] genericParameterTypes = targetMethod.getGenericParameterTypes();
        targetMethod.setAccessible(true);
        if (genericParameterTypes.length == 2) {
            targetMethod.invoke(bean, consumer, msg);
        } else if (genericParameterTypes.length == 1) {
            try {
                if (genericParameterTypes[0] instanceof Message) {
                    targetMethod.invoke(bean, msg);
                } else {
                    targetMethod.invoke(bean, msg.getValue());
                }
                consumer.acknowledge(msg);
            } catch (Exception e) {
                consumer.negativeAcknowledge(msg);
                throw new RuntimeException("消费者异常", e);
            }
        } else {
            log.error("bean={} targetMethod={} parameter error", bean.getClass(), targetMethod.getName());
        }
    }
}
