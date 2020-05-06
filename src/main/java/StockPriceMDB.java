import java.time.Duration;
import java.util.Collections;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.ConsumerFactory;
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
public class StockPriceMDB {

  public static void main(String[] args) {

    final ConsumerFactory consumerFactory = new ConsumerFactory();
    final ConsumerConfig<String, byte[]> consumerConfig = new ConsumerConfig<String, byte[]>()
        .setTopics(Collections.singletonList("stock_changed"))
        .setGroupId("stock_client")
        .withProp(MAX_POLL_INTERVAL_MS_CONFIG, (int) Duration.ofMinutes(1).toMillis())
        .withProp(GROUP_ID_CONFIG, "stock_client")
        .withProp(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .withProp(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .withProp(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())
        .setRetryPolicy(RetryPolicy
            .builder()
            .maxTries(3)
            .delay(Duration.ofSeconds(29))
            .build()
        )
        .setBatchCallback((consumer, records, e) -> {
          for (final ConsumerRecord<String, byte[]> record : records) {
            throw new RuntimeException("an error occurred");
//            log.info("key={}, value={}", record.key(), new String(record.value()));
          }
        });

    consumerFactory.consume(consumerConfig);
  }

//
//  void notifyStockUpdates(ScheduledExecution execution) {
//    producer.send(new ProducerRecord<>(
//        "stock_changed",
//        String.format("stock=PAGS, price=%.2f", Math.random() * 100)
//            .getBytes()
//    ));
//    log.info(
//        "status=scheduled, scheduled-fire-time={}, fire-time={}",
//        execution.getScheduledFireTime(),
//        execution.getFireTime()
//    );
//  }
}
