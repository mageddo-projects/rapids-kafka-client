package com.mageddo.kafka.client;

import java.util.List;

import org.apache.kafka.common.PartitionInfo;

public interface ThreadConsumer<K ,V> extends AutoCloseable {
  void start(List<PartitionInfo> partitions);
}
