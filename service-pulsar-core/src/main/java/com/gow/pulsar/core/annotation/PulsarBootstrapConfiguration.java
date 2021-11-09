package com.gow.pulsar.core.annotation;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

public class PulsarBootstrapConfiguration implements ImportBeanDefinitionRegistrar {
    public PulsarBootstrapConfiguration() {
    }

    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition("com.gow.pulsar.core.annotation.PulsarSubscribeAnnotationPostProcessor")) {
            registry.registerBeanDefinition("com.gow.pulsar.core.annotation.PulsarSubscribeAnnotationPostProcessor", new RootBeanDefinition(PulsarSubscribeAnnotationPostProcessor.class));
        }

    }
}
