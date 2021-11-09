package com.gow.pulsar.core.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author gow
 * @date 2021/7/20
 */
@Slf4j
public class SubscribeHolderRegistrar {

    private SubscribeHolderRegistry registry;
    private List<SubscribeHolder> subscribeHolders = new ArrayList<>();

    public void registerHolder(PulsarSubscribe pulsarSubscribe, String realName, String realTopic,
                               Method method, Object bean) {
        preCheck(pulsarSubscribe, method);
        SubscribeHolder subscribeHolder = new SubscribeHolder();
        subscribeHolder.setRealSubscribeName(realName);
        subscribeHolder.setTopic(realTopic);
        subscribeHolder.setPattern(pulsarSubscribe.pattern());
        subscribeHolder.setContainerId(pulsarSubscribe.containerId());
        subscribeHolder.setSubscriptionType(pulsarSubscribe.subscriptionType());
        subscribeHolder.setSchema(pulsarSubscribe.schema());
        subscribeHolder.setJsonClass(pulsarSubscribe.jsonClass());
        subscribeHolder.setHandlerMethod(method);
        subscribeHolder.setBean(bean);
        subscribeHolders.add(subscribeHolder);
    }

    private void preCheck(PulsarSubscribe pulsarSubscribe, Method method) {
        if (StringUtils.isEmpty(pulsarSubscribe.topic())) {
            throw new RuntimeException("pulsarSubscribe topic isEmpty");
        }

        if (StringUtils.isEmpty(pulsarSubscribe.subscriptionName())) {
            throw new RuntimeException("pulsarSubscribe subscriptionName isEmpty");
        }
        int length = method.getGenericParameterTypes().length;
        if (length <= 0 || length >= 3) {
            throw new RuntimeException("method=" + method.getName() + " parameterType error");
        }
    }

    public void setSubscribeHolderRegistry(SubscribeHolderRegistry subscribeHolderRegistry) {
        this.registry = subscribeHolderRegistry;
    }

    public void registerAllHolders() {
        log.info("start registerAllHolders size={}", subscribeHolders.size());
        subscribeHolders.forEach(subscribeHolder -> {
            registry.registerSubscribeContainer(subscribeHolder);
        });
    }
}
