package com.mageddo.kafka.client.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerRecordUtils {
  public static String toString(ConsumerRecord record) {
    if (record == null) {
      return null;
    }

    return String.format(
        "(topic=%s, partition=%d, offset=%d)",
        record.topic(), record.partition(), record.offset()
    );
  }
}
