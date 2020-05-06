package com.mageddo.kafka.client;

import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class Consumers {

  public static <K, V> void commitSyncRecord(Consumer<K, V> consumer, ConsumerRecord<K, V> record) {
    consumer.commitSync(Collections.singletonMap(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset())
    ));
  }

  public static <K, V> void doRecoverWhenAvailable(
      Consumer<K, V> consumer,
      ConsumingConfig<K, V> consumingConfig,
      ConsumerRecord<K, V> record
  ) {
    if (consumingConfig.getRecoverCallback() != null) {
      consumingConfig.getRecoverCallback().recover(record);
      commitSyncRecord(consumer, record);
    } else {
      log.warn("status=no recover callback was specified");
    }
  }
}
