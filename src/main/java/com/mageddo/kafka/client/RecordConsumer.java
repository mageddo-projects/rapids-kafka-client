package com.mageddo.kafka.client;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RecordConsumer<K, V> extends DefaultConsumer<K, V> {

  private final Consumer<K, V> consumer;
  private final ConsumerConfig<K,V> consumerConfig;

  @Override
  protected void consume(ConsumerRecords<K, V> records) {

    for (final ConsumerRecord<K, V> record : records) {
      final AtomicBoolean recovered = new AtomicBoolean();
      Retrier
          .builder()
          .retryPolicy(this.consumerConfig.retryPolicy())
          .onExhausted((lastFailure) -> {
            log.info("exhausted tries");
            doRecoverWhenAvailable(this.consumer, this.consumerConfig, record, lastFailure);
            recovered.set(true);
          })
          .onRetry(() -> {
            log.info("failed to consume");
            commitSyncRecord(this.consumer, record);
          })
          .build()
          .run(() -> {
            if (log.isTraceEnabled()) {
              log.info("status=consuming, record={}", record);
            }
            try {
              this.consumerConfig
                  .callback()
                  .accept(this.consumer, record, null);
            } catch (Exception e) {
              Exceptions.throwException(e);
            }
          });
      if (recovered.get()) {
        // pare o consumo para fazer poll imediatamente
        // e não chegar no timeout por não ter chamado poll
        // por causa das retentativas dessa mensagem
        return;
      }
    }
    this.consumer.commitSync();
  }

  @Override
  protected Consumer<K, V> consumer() {
    return this.consumer;
  }

  @Override
  protected ConsumerConfig<K, V> consumerConfig() {
    return this.consumerConfig;
  }

  @Override
  protected void onErrorCallback(Exception e) {
    try {
      this.consumerConfig
          .callback()
          .accept(this.consumer, null, e);
    } catch (Exception ex) {
      Exceptions.throwException(e);
    }
  }

}
