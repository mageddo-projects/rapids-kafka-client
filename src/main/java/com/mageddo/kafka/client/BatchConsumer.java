package com.mageddo.kafka.client;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchConsumer<K, V> extends DefaultConsumer<K, V> {

  private final Consumer<K, V> consumer;
  private final ConsumerConfig<K, V> consumerConfig;

  @Override
  protected void consume(ConsumerRecords<K, V> records) {
    final Retrier retrier = Retrier
        .builder()
        .retryPolicy(this.consumerConfig.retryPolicy())
        .onRetry(() -> {
          log.info("failed to consume");
          for (final TopicPartition partition : records.partitions()) {
            commitFirstRecord(this.consumer, records, partition);
          }
        })
        .onExhausted((lastFailure) -> {
          log.info("status=exhausted-tries, records={}", records.count());
          records.forEach(record -> this.doRecoverWhenAvailable(
              DefaultRecoverContext
              .<K, V>builder()
              .consumer(this.consumer)
              .lastFailure(lastFailure)
              .record(record)
              .build(),
              this.consumerConfig.recoverCallback()
          ));
        })
        .build();

    retrier.run(() -> {
      if (log.isTraceEnabled()) {
        log.trace("status=consuming, records={}", records);
      }
      try {
        this.consumerConfig
            .batchCallback()
            .accept(
                DefaultCallbackContext
                    .<K, V>builder()
                    .consumer(this.consumer)
                    .records(records)
                    .build(),
                records
            );
      } catch (Exception e) {
        Exceptions.throwException(e);
      }
    });
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

  private void commitFirstRecord(Consumer<K, V> consumer, ConsumerRecords<K, V> records, TopicPartition partition) {
    final ConsumerRecord<K, V> firstRecord = getFirstRecord(records, partition);
    if (firstRecord != null) {
      commitSyncRecord(consumer, firstRecord);
    }
  }

  private ConsumerRecord<K, V> getFirstRecord(ConsumerRecords<K, V> records, TopicPartition partition) {
    final List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
    return partitionRecords.isEmpty() ? null : partitionRecords.get(0);
  }

}
