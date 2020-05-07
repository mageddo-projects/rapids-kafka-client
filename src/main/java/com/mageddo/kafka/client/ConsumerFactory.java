package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

@Slf4j
public class ConsumerFactory<K, V> {

  public void consume(ConsumerConfig<K, V> consumerConfig) {

    this.checkReasonablePollInterval(consumerConfig);

    final KafkaConsumer<K, V> firstConsumer = create(consumerConfig);
    final List<Consumer<K, V>> consumers = createConsumers(consumerConfig, firstConsumer);
    final Deque<PartitionInfo> partitions = new LinkedList<>(getAllPartitions(firstConsumer, consumerConfig.getTopics()));
    final int partitionsByConsumer = calcPartitionsByConsumer(consumerConfig, partitions.size());
    for (final Consumer<K, V> consumer : consumers) {
      final List<PartitionInfo> consumerPartitions = popConsumerPartitions(partitions, partitionsByConsumer);
      if(consumerPartitions.isEmpty()){
        log.info("status=no-consumerPartitions-left, there will be idle consumers");
        break;
      }
      final ThreadConsumer<K, V> threadConsumer = getInstance(consumer, consumerConfig);
      threadConsumer.start(consumerPartitions);
    }
    log.info("status={} consumers started", consumers.size());
  }

  private int calcPartitionsByConsumer(ConsumerConfig<K, V> consumerConfig, int partitions) {
    return Math.max(1, partitions / consumerConfig.getConsumers());
  }

  private List<Consumer<K, V>> createConsumers(ConsumerConfig<K, V> consumerConfig, KafkaConsumer<K, V> firstConsumer) {
    final List<Consumer<K, V>> consumers = new ArrayList<>();
    consumers.add(firstConsumer);
    for (int i = 0; i < consumerConfig.getConsumers() - 1; i++) {
      consumers.add(create(consumerConfig));
    }
    return consumers;
  }

  private ThreadConsumer<K, V> getInstance(Consumer<K, V> consumer, ConsumerConfig<K, V> consumerConfig) {
    if(consumerConfig.getBatchCallback() != null){
      return new BatchConsumer<>(consumer, consumerConfig);
    }
    return new RecordConsumer<>(consumer, consumerConfig);
  }

  private List<PartitionInfo> popConsumerPartitions(Deque<PartitionInfo> stack, int quantity) {
    final List<PartitionInfo> partitions = new ArrayList<>();
    for (int i = 0; i < Math.min(quantity, stack.size()); i++) {
      partitions.add(stack.pop());
    }
    return partitions;
  }

  private List<PartitionInfo> getAllPartitions(KafkaConsumer<K, V> consumer, Collection<String> topics) {
    final List<PartitionInfo> partitions = new ArrayList<>();
    for (final String topic : topics) {
      partitions.addAll(consumer.partitionsFor(topic));
    }
    return partitions;
  }

  private KafkaConsumer<K, V> create(ConsumerConfig<K, V> consumerConfig) {
    return new KafkaConsumer<>(consumerConfig.getProps());
  }

  private void checkReasonablePollInterval(ConsumerConfig<K, V> consumerConfig) {
    final int defaultPollInterval = (int) Duration
        .ofMinutes(5)
        .toMillis();

    final int currentPollInterval = (int) consumerConfig
        .getProps()
        .getOrDefault(MAX_POLL_INTERVAL_MS_CONFIG, defaultPollInterval);

    final RetryPolicy retryPolicy = consumerConfig.getRetryPolicy();

    final long retryMaxWaitTime = retryPolicy
        .calcMaxTotalWaitTime()
        .toMillis();

    if (currentPollInterval < retryMaxWaitTime) {
      log.warn(
          "msg=your 'max.poll.interval.ms' is set to a value less than the retry policy, it will cause consumer "
              + "rebalancing, increase 'max.poll.interval.ms' or decrease the retry policy delay or retries, "
              + "max.poll.interval.ms={}, retryMaxWaitTime={} (retries={}, delay={})",
          Duration.ofMillis(currentPollInterval),
          Duration.ofMillis(retryMaxWaitTime),
          retryPolicy.getMaxTries(),
          retryPolicy.getDelay()
      );
    }
  }
}
