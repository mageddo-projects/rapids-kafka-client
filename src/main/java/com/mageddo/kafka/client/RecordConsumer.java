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
  private final Consumers<K, V> consumers;

  @Override
  protected void consume(ConsumerRecords<K, V> records) {

    for (final ConsumerRecord<K, V> record : records) {
      final AtomicBoolean recovered = new AtomicBoolean();
      Retrier
          .builder()
          .retryPolicy(this.consumers.retryPolicy())
          .onExhausted((lastFailure) -> {
            log.info("exhausted tries");
            this.doRecoverWhenAvailable(
                DefaultRecoverContext
                    .<K, V>builder()
                    .consumer(this.consumer)
                    .lastFailure(lastFailure)
                    .record(record)
                    .build()
                ,
                this.consumers.recoverCallback()
            );
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
              this.consumers
                  .callback()
                  .accept(
                      DefaultContext
                          .<K, V>builder()
                          .consumer(this.consumer)
                          .records(records)
                          .record(record)
                          .build(),
                      record
                  );
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
  protected Consumers<K, V> consumerConfig() {
    return this.consumers;
  }
}
