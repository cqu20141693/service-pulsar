package com.gow.pulsar.core.annotation;

import com.gow.pulsar.core.domain.PulsarSchemaType;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.pulsar.client.api.SubscriptionType;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PulsarSubscribe {

    String topic();

    boolean pattern() default false;

    String subscriptionName();

    PulsarSchemaType schema() default PulsarSchemaType.String;

    Class<?> jsonClass() default Object.class;

    /**
     * 容器beanName
     */
    String containerId() default "";

    SubscriptionType subscriptionType() default SubscriptionType.Failover;

}
