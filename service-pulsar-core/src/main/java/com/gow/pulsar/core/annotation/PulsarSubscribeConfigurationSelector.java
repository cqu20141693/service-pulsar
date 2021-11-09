package com.gow.pulsar.core.annotation;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.type.AnnotationMetadata;

public class PulsarSubscribeConfigurationSelector implements DeferredImportSelector {
    public PulsarSubscribeConfigurationSelector() {
    }

    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[]{PulsarBootstrapConfiguration.class.getName()};
    }
}
