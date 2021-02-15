package com.mageddo.kafka.client;

import java.util.concurrent.atomic.AtomicBoolean;

import com.mageddo.kafka.client.internal.ObjectsUtils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.mageddo.kafka.client.DefaultConsumingConfig.DEFAULT_RETRY_STRATEGY;

@Slf4j
@RequiredArgsConstructor
public class RecordConsumer<K, V> extends DefaultConsumer<K, V> {

  private final Consumer<K, V> consumer;
  private final ConsumerConfig<K, V> consumerConfig;

  @Override
  protected void consume(ConsumerRecords<K, V> records) {

    for (final ConsumerRecord<K, V> record : records) {
      final AtomicBoolean recovered = new AtomicBoolean();
      Retrier
          .builder()
          .retryPolicy(this.getRetryPolicy())
          .onExhausted((lastFailure) -> {
            if(log.isDebugEnabled()){
              log.debug("exhausted tries");
            }
            this.doRecoverWhenAvailable(
                DefaultRecoverContext
                    .<K, V>builder()
                    .consumer(this.consumer)
                    .lastFailure(lastFailure)
                    .record(record)
                    .build()
                ,
                this.consumerConfig.recoverCallback()
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
              this.consumerConfig
                  .callback()
                  .accept(
                      DefaultCallbackContext
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
  protected ConsumerConfig<K, V> consumerConfig() {
    return this.consumerConfig;
  }

  RetryPolicy getRetryPolicy() {
    return ObjectsUtils.firstNonNull(this.consumerConfig.retryPolicy(), DEFAULT_RETRY_STRATEGY);
  }

}
