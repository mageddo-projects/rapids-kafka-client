package com.mageddo.kafka.client;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecordConsumer<K, V> {

  public void consume(
      Consumer<K, V> consumer,
      ConsumingConfig<K, V> consumingConfig,
      ConsumerRecords<K, V> records
  ) {

    for (final ConsumerRecord<K, V> record : records) {
      final AtomicBoolean recovered = new AtomicBoolean();
      Retrier
          .builder()
          .onExhausted((lastFailure) -> {
            log.info("exhausted tries");
            Consumers.doRecoverWhenAvailable(consumer, consumingConfig, record, lastFailure);
            recovered.set(true);
          })
          .onRetry(() -> {
            log.info("failed to consume");
            Consumers.commitSyncRecord(consumer, record);
          })
          .build()
          .run(() -> {
            if (log.isTraceEnabled()) {
              log.info("status=consuming, record={}", record);
            }
            consumingConfig
                .getCallback()
                .accept(consumer, record, null);
          });
      if (recovered.get()) {
        // pare o consumo para fazer poll imediatamente
        // e não chegar no timeout por não ter chamado poll
        // por causa das retentativas dessa mensagem
        return;
      }
    }
  }

}
