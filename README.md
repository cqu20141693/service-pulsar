### pulsar starter

1. introduction

``` 
puslsar starter provides simple use of pulsar for production and consumption data
```

### examples

1. dependency and config

```
    <dependency>
      <groupId>com.chongctech</groupId>
      <artifactId>service-pulsar-starter</artifactId>
      <version>v1.0.0-SNAPSHOT</version>
    </dependency>

com:
  chongctech:
    service:
      pulsar:
        # must
        service-url: pulsar://172.30.203.25:6650,172.30.203.26:6650,172.30.203.24:6650
        # option
        cluster: pulsar-cluster
        # must
        client:
          #option
          role: gow
          # must
          jwt-token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjY3RlY2gifQ.TkCwKZIW-CCQEI6qjoAnEImpJzDymkYthWUymGQwArg
        # option : The producer can be configured, otherwise the default configuration
        producer:
          # option : It will be created when it is configured
          topics:
            # topic
            - name: persistent://cctech/devices/checked-cmd-topic
            - name: persistent://cctech/devices/checked-data-topic
            - name: persistent://cctech/devices/ota-event-topic
            - name: persistent://cctech/devices/sys-log-topic
            - name: persistent://cctech/devices/subscribe-event-topic
          # option
          access-mode: shared
          batching-enabled: true
        # option : The consumer can be configured, otherwise the default configuration
        consumer:
          subscription-initial-position: latest
          receiver-queue-size: 2000
          
notes: must configuration must be configured, option means configurable
```

1. producer

``` 
The producer recommends just using String scheam:

    @Autowired
    private StringProducerTemplate stringProducerTemplate;

API:

    public MessageId send(String topic, String value) 
    public MessageId send(String topic, String key, String value) 
    public MessageId send(ProducerRecord<String> record)
    ......

```

2. consumer

``` 
The consumer recommends just using String scheam:

@EnablePulsar : Use on configuration class 

@PulsarSubscribe: subscribe topic to consumer message

```

3. java demo

``` 
@SpringBootApplication
@EnablePulsar
public class PulsarStarter {
    public static void main(String[] args) {
        SpringApplication.run(PulsarStarter.class, args);
    }
}

@Component
@Slf4j
public class BizPublisher implements CommandLineRunner {
    @Autowired
    private ProducerTemplate producerTemplate;


    @Override
    public void run(String... args) throws Exception {
           producerTemplate.send("test-topic","key","value");
    }
}

@Component
@Slf4j
public class BizSubscribe {
    @PulsarSubscribe(topic = "${pulsar.topic.in.fail-over}", subscriptionName =
            "pulsar-subscribe-consumer", schema = PulsarSchemaType.String,
            containerId = "", subscriptionType = SubscriptionType.Failover)
    public void receive(Consumer<String> consumer, Message<String> msg) throws PulsarClientException {
    try{
        log.info("manualCommit receive String data={}", msg.getValue());
        consumer.acknowledge(msg);
        }  catch (Exception e) {
            e.printStackTrace();
            consumer.negativeAcknowledge(msg);
        }
    }
    
        @PulsarSubscribe(topic = "${pulsar.topic.in.fail-over}", subscriptionName =
            "pulsar-subscribe-consumer", schema = PulsarSchemaType.String,
            containerId = "", subscriptionType = SubscriptionType.Failover)
    public void receiveString(Stringmsg) throws PulsarClientException {
        log.info("manualCommit receive String data={}", msg.getValue());
     }
    
}

```

