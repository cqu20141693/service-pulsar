package com.chongctech.pulsar.config;

import com.chongctech.pulsar.core.container.PulsarContainer;
import com.chongctech.pulsar.core.domain.PulsarProperties;
import com.chongctech.pulsar.core.factory.PulsarFactory;
import com.chongctech.pulsar.core.producer.ProducerTemplate;
import com.chongctech.pulsar.core.producer.StringProducerTemplate;
import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.task.TaskSchedulerBuilder;
import org.springframework.boot.task.TaskSchedulerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @author gow
 * @date 2021/7/2
 */
@ConditionalOnClass(PulsarClient.class)
@ConditionalOnProperty(value = {"com.chongctech.service.pulsar.service-url"})
public class PulsarAutoConfiguration {

    @Bean
    public PulsarFactory pulsarFactory() {
        return new PulsarFactory();
    }

    @Bean(destroyMethod = "close")
    public PulsarClient pulsarClient(PulsarProperties properties, PulsarFactory pulsarFactory)
            throws PulsarClientException {

        return pulsarFactory.createClient(properties.getServiceUrl(), properties.getClient());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = {"com.chongctech.service.pulsar.admin.web-service-url"})
    public PulsarAdmin pulsarAdmin(PulsarProperties properties) throws PulsarClientException {
        return PulsarAdmin.builder()
                .authentication(AuthenticationFactory.token(properties.getAdmin().getAdminToken()))
                .serviceHttpUrl(properties.getAdmin().getWebServiceUrl())
                .build();
    }

    @Bean("pulsarContainer")
    public PulsarContainer pulsarContainer(PulsarProperties properties, PulsarFactory pulsarFactory,
                                           PulsarClient client) {
        return new PulsarContainer(properties, pulsarFactory, client);
    }

    @Bean("producerTemplate")
    public ProducerTemplate producerTemplate(@Qualifier("pulsarContainer") PulsarContainer container) {
        return new ProducerTemplate(container);
    }

    @Bean("stringProducerTemplate")
    public StringProducerTemplate stringProducerTemplate(@Qualifier("pulsarContainer") PulsarContainer container) {
        return new StringProducerTemplate(container);
    }

    public TaskScheduler defaultTaskScheduler(ObjectProvider<TaskSchedulerCustomizer> taskSchedulerCustomizers) {

        TaskSchedulerBuilder builder = new TaskSchedulerBuilder();
        ThreadPoolTaskScheduler taskScheduler = builder.poolSize(2)
                .awaitTermination(true)
                .awaitTerminationPeriod(Duration.ofSeconds(60))
                .threadNamePrefix("pulsar-scheduler-task-")
                .customizers(taskSchedulerCustomizers).build();
        taskScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskScheduler.initialize();
        return taskScheduler;
    }
}