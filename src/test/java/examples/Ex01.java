package examples;

import java.time.Duration;

import com.mageddo.kafka.client.ConsumerConfigDefault;
import com.mageddo.kafka.client.RetryPolicy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

@Slf4j
public class Ex01 {

  public static void main(String[] args) throws InterruptedException {

    TopicMessageSender.keepSendingMessages();

    ConsumerConfigDefault.<String, byte[]>builder()
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
        .recoverCallback((ctx) -> {
          log.info("status=recovering, record={}", new String(ctx.record().value()));
        })
        .batchCallback((ctx, records) -> {
          for (final ConsumerRecord<String, byte[]> record : records) {
            final double randomValue = Math.random();
//            if(randomValue > 0.5 && randomValue < 0.8){
//              throw new RuntimeException("an error occurred");
//            }
            log.info("key={}, value={}", record.key(), new String(record.value()));
          }
        })
        .build()
        .consume()
    ;

    log.info("waiting termination....");
    Thread.currentThread()
        .join();
    log.info("exiting......");
  }

}
