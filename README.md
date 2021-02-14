![](https://i.imgur.com/QsI0TKc.png)

# Rapids Kafka Client

Kafka Client is a vanilla java library that makes it easy to consume data from kafka,
 a list of features:

* [x] Parallel consuming
* [x] Consuming retry
* [x] Consuming failover
* [x] Designed to be easy to mock and test
* [x] Designed to support slow consumers without kafka re balancing
* [x] Designed to high throughput usage
* [x] Individual record consuming
* [x] Batch records consuming
* [x] Frameworkless, but easily configurable to someone
* [x] Commits are managed for you based on behavior

# Getting Started

```groovy
compile("com.mageddo.rapids-kafka-client:rapids-kafka-client:1.0.3")
```

```java
Consumers.<String, String>builder()
.prop(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
.prop(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
.prop(GROUP_ID_CONFIG, "stocks")
.topics("stock_changed")
.recoverCallback(ctx -> {
  // here you can send the message to another topic, send a SMS, etc.
  log.info("status=recovering, value={}", ctx.record().value());
})
.callback((ctx, record) -> {
  log.info("status=consumed, value={}", record.value());
})
.build()
.consume()
.waitFor();
```

# Examples
* [Vanilla][1]
* [Spring Framework][2]
* [Micronaut][3]
* [Quarkus][4]

[1]: https://github.com/mageddo-projects/kafka-client-examples/tree/master/vanilla
[2]: https://github.com/mageddo-projects/kafka-client-examples/tree/master/spring
[3]: https://github.com/mageddo-projects/kafka-client-examples/tree/master/micronaut
[4]: https://github.com/mageddo-projects/kafka-client-examples/tree/master/quarkus
