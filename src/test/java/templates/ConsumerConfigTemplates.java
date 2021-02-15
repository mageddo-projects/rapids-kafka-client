package templates;

import java.time.Duration;
import java.util.Collections;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.ConsumerConfigDefault;
import com.mageddo.kafka.client.RetryPolicy;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerConfigTemplates {

  public static <K, V> ConsumerConfigDefault<K, V> build() {
    return ConsumerConfigTemplates.<K, V>builder().build();
  }

  public static <K, V> ConsumerConfigDefault.Builder<K, V> builder() {
    return ConsumerConfig.<K, V>builder()
        .batchCallback((ctx, records) -> System.out.println("batch callback"))
        .callback((ctx, record) -> System.out.println("callback"))
        .consumers(3)
        .retryPolicy(RetryPolicy
            .builder()
            .maxTries(2)
            .delay(Duration.ofMinutes(1))
            .build()
        )
        .recoverCallback((ctx) -> System.out.println("recover callback"))
        .topics(Collections.singleton("fruit_topic"))
        .pollInterval(Duration.ofMillis(1000 / 3))
        .pollTimeout(Duration.ofMillis(100))
        ;
  }

}
