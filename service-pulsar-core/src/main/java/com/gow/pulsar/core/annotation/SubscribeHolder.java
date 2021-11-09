package com.gow.pulsar.core.annotation;

import com.gow.pulsar.core.domain.PulsarSchemaType;
import java.lang.reflect.Method;
import lombok.Data;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * @author gow
 * @date 2021/7/20
 */
@Data
public class SubscribeHolder {


    private String topic;
    private Boolean pattern;
    private String containerId;
    private String realSubscribeName;
    private SubscriptionType subscriptionType;
    private PulsarSchemaType schema;
    private Class<?> jsonClass;
    private Method handlerMethod;
    private Object bean;
}
