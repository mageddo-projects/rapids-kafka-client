package examples;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.ConsumerFactory;
import com.mageddo.kafka.client.RetryPolicy;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Slf4j
public class Ex01 {

  public static void main(String[] args) throws InterruptedException {

    TopicMessageSender.keepSendingMessages();

    final ConsumerFactory<String, byte[]> consumerFactory = new ConsumerFactory<>();
    final ConsumerConfig<String, byte[]> consumerConfig = ConsumerConfig.<String, byte[]>builder()
        .prop(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .prop(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .prop(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())

        .topics("stock_client_v2")
        .prop(GROUP_ID_CONFIG, "stock_client")
        .consumers(11)
        .retryPolicy(RetryPolicy
            .builder()
            .maxTries(1)
            .delay(Duration.ofSeconds(29))
            .build()
        )
        .recoverCallback((record, lastFailure) -> {
          log.info("status=recovering, record={}", new String(record.value()));
        })
        .batchCallback((consumer, records, e) -> {
          for (final ConsumerRecord<String, byte[]> record : records) {
            final double randomValue = Math.random();
//            if(randomValue > 0.5 && randomValue < 0.8){
//              throw new RuntimeException("an error occurred");
//            }
            log.info("key={}, value={}", record.key(), new String(record.value()));
          }
        })
        .build();

    consumerFactory.consume(consumerConfig);
    log.info("waiting termination....");
    Thread.currentThread().join();
    log.info("exiting......");
  }

}
