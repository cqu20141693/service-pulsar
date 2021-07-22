package com.chongctech.pulsar.core.annotation;

import com.chongctech.pulsar.core.container.PulsarContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @author gow
 * @date 2021/7/20
 */
@Component
@Slf4j
public class SubscribeHolderRegistry implements ApplicationContextAware, BeanFactoryAware {

    private final String DEFAULT_CONTAINER = "pulsarContainer";
    private ApplicationContext applicationContext;

    private BeanFactory beanFactory;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public void registerSubscribeContainer(SubscribeHolder holder) {
        String containerId = holder.getContainerId();
        if (StringUtils.isEmpty(containerId)) {
            containerId = DEFAULT_CONTAINER;
        }
        try {
            PulsarContainer container = beanFactory.getBean(containerId, PulsarContainer.class);
            container.addConsumer(holder, holder.getHandlerMethod(), holder.getBean());
        } catch (NoSuchBeanDefinitionException e) {
            log.warn("containerId={} not exist", containerId);
            throw e;
        }
    }
}
