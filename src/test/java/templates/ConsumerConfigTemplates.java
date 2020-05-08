package templates;

import java.time.Duration;
import java.util.Collections;

import com.mageddo.kafka.client.Consumers;
import com.mageddo.kafka.client.RetryPolicy;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerConfigTemplates {

  public static <K, V> Consumers<K, V> build() {
    return ConsumerConfigTemplates.<K, V>builder().build();
  }

  public static <K, V> Consumers.ConsumersBuilder<K, V> builder() {
    return Consumers.<K, V>builder()
        .batchCallback((ctx, records) -> System.out.println("batch callback"))
        .callback((ctx, record) -> System.out.println("callback"))
        .consumers(3)
        .pollInterval(Duration.ofMillis(1000 / 3))
        .retryPolicy(RetryPolicy
            .builder()
            .maxTries(2)
            .delay(Duration.ofMinutes(1))
            .build()
        )
        .recoverCallback((record, lastFailure) -> System.out.println("recover callback"))
        .topics(Collections.singleton("fruit_topic"))
        .pollTimeout(Duration.ofMillis(100))
        ;
  }

}
