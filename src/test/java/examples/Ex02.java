package examples;

import java.time.Duration;

import com.mageddo.kafka.client.Consumers;
import com.mageddo.kafka.client.RetryPolicy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Slf4j
public class Ex02 {

  public static void main(String[] args) {

    TopicMessageSender.keepSendingMessages();

    defaultConfig()
        .topics("stock_client_v2")
        .prop(GROUP_ID_CONFIG, "stock_client")
        .consumers(1)
        .retryPolicy(RetryPolicy
            .builder()
            .maxTries(1)
            .delay(Duration.ofSeconds(29))
            .build()
        )
        .build()
        .batchConsume((ctx, records) -> {
              for (final ConsumerRecord<String, byte[]> record : records) {
                log.info("key={}, value={}", record.key(), new String(record.value()));
              }
            },
            (record, lastFailure) -> {
              log.info("status=recovering, record={}", new String(record.value()));
            }
        )
        .waitFor();
  }

  private static Consumers.ConsumersBuilder<String, byte[]> defaultConfig() {
    return Consumers.<String, byte[]>builder()
        .prop(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .prop(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .prop(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())
        .prop(MAX_POLL_INTERVAL_MS_CONFIG, (int) Duration.ofMinutes(5)
            .toMillis());
  }

}
