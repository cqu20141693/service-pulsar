### pulsar starter

https://mp.weixin.qq.com/s/PiJuN5XPkXt-1tly__I-nA
#### client 配置
``` 
配置jwtToken 用于连接身份认证
配置numIoThreads : 配置平台broker数量3
listenerThreads: cqu核心数量*2 和消费者数量小者
内存限制: 64 MB
操作超时： 5 s
连接超时： 10 s

```
#### producer 实现
``` 
默认实现StringProducerTemplate,并设计了各种send接口，ProducerRecord包括topic,key,value，timestamp参数
topic用于选择producerClient,key用于路由分区，value表示数据，tiemstamp 表示时间戳

本地会维护一个topic到ProducerClient的映射到container中，用于send时重复使用，而不用重新创建
// 支持配置生产topic ，服务启动会提前创建好producer client :
container implment SmartInitializingSingleton ,在after 单例实例初始化后后会读取配置注册生产者
// 支持优雅关闭producer client： 
container 实现了smartLifecycle ,在spring boot 退出前进行销毁

需要配置container bean和StringProducerTemplate bean

使用时直接注入StringProducerTemplate bean进行数据发送，
发送有同步发送和异步发送，异步发送返回completableFuture，
使用者处理，一般是重试n次，打印日志，也可失败写入死信队列
同时支持延迟发送和定时发送做延迟任务，只支持share模式的订阅者进行消费

生产者配置：
默认不存在key时，轮询写入分区,配置存在key时，相同key写入单分区
默认的key hash位JavaStringHash
批量enable true
批量发送最大延迟100 ms
最大批量消息1000

```
#### consumer 
```  
消费者配置：
公共配置： 
subscriptionInitialPosition=Latest
receiverQueueSize=1000
negativeAckRedeliveryDelayMicros=1 min
batchIndexAcknowledgmentEnabled=true
私有配置：
topic
subscriptionName
subscriptionType: 
Exclusive,Failover 支持批量提交
Shared，Key_Shared 只支持individualCommit

实现方式
```

#### pulsar 应用
1. 利用pulsar 做延迟消息
   ``` 
   producer 支持延迟发送和定时间点发送消息
   
   ```
2. pulsar 做消息通知
3. pulsar 做数据流转中间件
4. pulsar 做多租户云订阅

