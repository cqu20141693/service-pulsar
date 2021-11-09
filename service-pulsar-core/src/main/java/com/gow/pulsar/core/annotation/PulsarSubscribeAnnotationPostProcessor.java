package com.gow.pulsar.core.annotation;

import com.gow.pulsar.core.utils.SubscribeNameGenerator;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

@Slf4j
public class PulsarSubscribeAnnotationPostProcessor
        implements BeanPostProcessor, Ordered, BeanFactoryAware, ApplicationListener<ApplicationReadyEvent>,
        EnvironmentAware {

    private Environment environment;
    private BeanFactory beanFactory;
    private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap(64));
    private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();
    private BeanExpressionContext expressionContext;
    private SubscribeHolderRegistrar registrar = new SubscribeHolderRegistrar();

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
        }
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        SubscribeHolderRegistry holderRegistry = this.beanFactory.getBean(SubscribeHolderRegistry.class);
        registrar.setSubscribeHolderRegistry(holderRegistry);
        registrar.registerAllHolders();
    }

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return BeanPostProcessor.super.postProcessBeforeInitialization(bean, beanName);
    }

    private void processPulsarSubscribe(PulsarSubscribe subscribe, Method method, Object bean, String beanName) {
        String realName = subscribe.subscriptionName();
        SubscribeNameGenerator generator = SubscribeNameGenerator.parseFromCode(realName);
        if (generator != SubscribeNameGenerator.UNKNOWN) {
            // ip
            String name = generator.getName();

            String appName = environment.getProperty("spring.application.name");
            if (appName == null) {
                realName = name;
            } else {
                realName = appName + "-" + name;
            }
        } else if (StringUtils.hasText(realName)) {
            Object resolvedName = this.resolveExpression(realName);
            if (resolvedName instanceof String) {
                realName = (String) resolvedName;
            }
        }

        String realTopic = subscribe.topic();
        if (StringUtils.hasText(realTopic)) {
            Object resolvedTopic = this.resolveExpression(realTopic);
            if (resolvedTopic instanceof String) {
                realTopic = (String) resolvedTopic;
            }
        }
        log.info("listener topic={},subscriptionName={},method={},bean={},beanName={}", realTopic, realName,
                method.getName(),
                bean.getClass(),
                beanName);

        registrar.registerHolder(subscribe, realName, realTopic, method, bean);
    }

    private Object resolveExpression(String value) {
        return this.resolver.evaluate(this.resolve(value), this.expressionContext);
    }

    private String resolve(String value) {
        return this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory ?
                ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value) : value;
    }


    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
            Class<?> targetClass = AopUtils.getTargetClass(bean);

            Map<Method, Set<PulsarSubscribe>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                    (MethodIntrospector.MetadataLookup<Set<PulsarSubscribe>>) (methodx) -> {
                        Set<PulsarSubscribe> listenerMethods = this.findSubscribeAnnotations(methodx);
                        return !listenerMethods.isEmpty() ? listenerMethods : null;
                    });

            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
                log.trace("No @PulsarSubscribe annotations found on bean type: " + bean.getClass());
            } else {
                // Non-empty set of methods
                for (Map.Entry<Method, Set<PulsarSubscribe>> entry : annotatedMethods.entrySet()) {
                    Method method = entry.getKey();
                    for (PulsarSubscribe listener : entry.getValue()) {
                        processPulsarSubscribe(listener, method, bean, beanName);
                    }
                }
            }
        }
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }

    private Set<PulsarSubscribe> findSubscribeAnnotations(Method method) {
        Set<PulsarSubscribe> listeners = new HashSet();
        PulsarSubscribe ann =
                (PulsarSubscribe) AnnotatedElementUtils.findMergedAnnotation(method, PulsarSubscribe.class);
        if (ann != null) {
            listeners.add(ann);
        }

        return listeners;
    }

}
