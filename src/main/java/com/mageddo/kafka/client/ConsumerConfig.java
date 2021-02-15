package com.mageddo.kafka.client;

/**
 * Definition to create a Kafka Consumer, you can create your first consumer starting here, see an example below
 *
 * <pre>
 * ConsumerConfig.&#x3C;String, String&#x3E;builder()
 * .topics("stock_changed")
 * .prop(GROUP_ID_CONFIG, "stocks")
 * .prop(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
 * .prop(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
 * .callback((ctx, record) -&#x3E; {
 *   log.info("status=consumed, value={}", record.value());
 * })
 * .recoverCallback(ctx -&#x3E; {
 *   // here you can send the message to another topic, send a SMS, etc.
 *   log.info("status=recovering, value={}", ctx.record().value());
 * })
 * .build()
 * .consume()
 * .waitFor();
 * </pre>
 *
 * When working with multiple consumers, there are better approaches though,
 * see {@link ConsumerStarter}, for more details.
 *
 * @param <K> ConsumerRecord key type
 * @param <V> ConsumerRecord value type
 *
 * @see org.apache.kafka.clients.consumer.ConsumerRecord
 * @see ConsumerStarter
 */
public interface ConsumerConfig<K, V> extends ConsumerCreateConfig<K, V>, ConsumingConfig<K, V> {
  static <K, V> ConsumerConfigDefault.Builder<K, V> builder() {
    return ConsumerConfigDefault.builder();
  }
}
