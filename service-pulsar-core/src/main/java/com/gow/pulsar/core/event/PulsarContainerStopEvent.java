package com.gow.pulsar.core.event;

import org.springframework.context.ApplicationEvent;

public class PulsarContainerStopEvent extends ApplicationEvent {

    public PulsarContainerStopEvent(Object source) {
        super(source);
    }
}
