package com.mageddo.kafka.client;

import java.util.List;

import org.apache.kafka.common.PartitionInfo;

public interface ThreadConsumer<K ,V> {
  void start(List<PartitionInfo> partitions);
}
