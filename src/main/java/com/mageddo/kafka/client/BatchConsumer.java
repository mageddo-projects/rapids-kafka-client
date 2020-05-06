package com.mageddo.kafka.client;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchConsumer<K, V> {

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
        .onExhausted(() -> {
          log.info("status=exhausted-tries, records={}", records.count());
          records.forEach(record -> Consumers.doRecoverWhenAvailable(consumer, consumingConfig, record));
        })
        .build();

    retrier.run(() -> {
      if (log.isTraceEnabled()) {
        log.debug("status=consuming, records={}", records);
      }
      consumingConfig
          .getBatchCallback()
          .accept(consumer, records, null);
    });
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
