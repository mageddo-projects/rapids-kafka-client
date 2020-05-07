package com.mageddo.kafka.client;

import java.util.List;

import lombok.RequiredArgsConstructor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchConsumer<K, V> extends DefaultConsumer<K, V> {

  private final Consumer<K, V> consumer;
  private final ConsumerConfig<K, V> consumerConfig;

  @Override
  public void consume(
      Consumer<K, V> consumer,
      ConsumingConfig<K, V> consumingConfig,
      ConsumerRecords<K, V> records
  ) {
    final Retrier retrier = Retrier
        .builder()
        .retryPolicy(consumingConfig.getRetryPolicy())
        .onRetry(() -> {
          log.info("failed to consume");
          for (final TopicPartition partition : records.partitions()) {
            commitFirstRecord(consumer, records, partition);
          }
        })
        .onExhausted((lastFailure) -> {
          log.info("status=exhausted-tries, records={}", records.count());
          records.forEach(record -> Consumers.doRecoverWhenAvailable(consumer, consumingConfig, record, lastFailure));
        })
        .build();

    retrier.run(() -> {
      if (log.isTraceEnabled()) {
        log.trace("status=consuming, records={}", records);
      }
      consumingConfig
          .getBatchCallback()
          .accept(consumer, records, null);
    });
  }

  @Override
  Consumer<K, V> consumer() {
    return this.consumer;
  }

  private void commitFirstRecord(Consumer<K, V> consumer, ConsumerRecords<K, V> records, TopicPartition partition) {
    final ConsumerRecord<K, V> firstRecord = getFirstRecord(records, partition);
    if (firstRecord != null) {
      Consumers.commitSyncRecord(consumer, firstRecord);
    }
  }

  private ConsumerRecord<K, V> getFirstRecord(ConsumerRecords<K, V> records, TopicPartition partition) {
    final List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
    return partitionRecords.isEmpty() ? null : partitionRecords.get(0);
  }

}
