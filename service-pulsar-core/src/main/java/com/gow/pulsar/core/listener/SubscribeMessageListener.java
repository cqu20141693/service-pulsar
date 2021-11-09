package com.gow.pulsar.core.listener;

import com.gow.pulsar.core.container.ack.AckStrategy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

/**
 * @author gow
 * @date 2021/7/20
 */
@Slf4j
public class SubscribeMessageListener<T> extends BaseMessageListener<T> {
    public SubscribeMessageListener(Method targetMethod, Object bean,
                                    AckStrategy ackStrategy) {
        super(targetMethod, bean, ackStrategy);
    }

    public void doInvoke(Consumer<T> consumer, Message<T> msg)
            throws IllegalAccessException, InvocationTargetException {
        Type[] genericParameterTypes = targetMethod.getGenericParameterTypes();
        //If the method is private, set its access permission to public
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
    }
}
