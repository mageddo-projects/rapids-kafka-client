package com.mageddo.kafka.client;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;

public interface ThreadConsumer<K ,V> {
  void start(List<PartitionInfo> partitions);
}
