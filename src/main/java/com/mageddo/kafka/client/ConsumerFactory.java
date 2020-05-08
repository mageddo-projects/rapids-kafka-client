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

    final Consumer<K, V> firstConsumer = create(consumerConfig);
    final Deque<PartitionInfo> partitions = new LinkedList<>(getAllPartitions(firstConsumer, consumerConfig.topics()));
    final int partitionsByConsumer = calcPartitionsByConsumer(consumerConfig, partitions.size());

    int startedConsumers = 1;
    for (int i = 0; i < consumerConfig.consumers() - 1; i++) {

      final List<PartitionInfo> consumerPartitions = popConsumerPartitions(partitions, partitionsByConsumer);
      if(consumerPartitions.isEmpty()){
        log.info("status=no-consumerPartitions-left, there will be idle consumers");
        break;
      }
      final Consumer<K, V> consumer = create(consumerConfig);
      final ThreadConsumer<K, V> threadConsumer = getInstance(consumer, consumerConfig);
      threadConsumer.start(consumerPartitions);

      i++;
    }
    final List<PartitionInfo> consumerPartitions = popConsumerPartitions(partitions, Integer.MAX_VALUE);
    if(consumerPartitions.isEmpty()){
      log.debug("status=no-consumer-partitions-left-for-first-consumer");
      return ;
    }
    final ThreadConsumer<K, V> threadConsumer = getInstance(firstConsumer, consumerConfig);
    threadConsumer.start(consumerPartitions);
    log.info("status={} consumers started", startedConsumers);
  }

  /**
   * @return true if terminated the available partitions
   */
  @Deprecated
  private boolean assignPartitionsAndStart(
      Consumer<K, V> preCreatedConsumer,
      ConsumerConfig<K, V> consumerConfig,
      Deque<PartitionInfo> partitions,
      int partitionsByConsumer
  ) {
    final List<PartitionInfo> consumerPartitions = popConsumerPartitions(partitions, partitionsByConsumer);
    if(consumerPartitions.isEmpty()){
      log.info("status=no-consumerPartitions-left, there will be idle consumers");
      return true;
    }
    final Consumer<K, V> consumer = preCreatedConsumer != null ? preCreatedConsumer : create(consumerConfig);
    final ThreadConsumer<K, V> threadConsumer = getInstance(consumer, consumerConfig);
    threadConsumer.start(consumerPartitions);
    return false;
  }

  private int calcPartitionsByConsumer(ConsumerConfig<K, V> consumerConfig, int partitions) {
    return Math.max(1, partitions / consumerConfig.consumers());
  }

  @Deprecated
  private List<Consumer<K, V>> createConsumers(ConsumerConfig<K, V> consumerConfig, Consumer<K, V> firstConsumer) {
    final List<Consumer<K, V>> consumers = new ArrayList<>();
    consumers.add(firstConsumer);
    for (int i = 0; i < consumerConfig.consumers() - 1; i++) {
      consumers.add(create(consumerConfig));
    }
    return consumers;
  }

  ThreadConsumer<K, V> getInstance(Consumer<K, V> consumer, ConsumerConfig<K, V> consumerConfig) {
    if(consumerConfig.batchCallback() != null){
      return new BatchConsumer<>(consumer, consumerConfig);
    }
    return new RecordConsumer<>(consumer, consumerConfig);
  }

  private List<PartitionInfo> popConsumerPartitions(Deque<PartitionInfo> stack, int quantity) {
    final List<PartitionInfo> partitions = new ArrayList<>();
    final int count = Math.min(quantity, stack.size());
    for (int i = 0; i < count; i++) {
      partitions.add(stack.pop());
    }
    return partitions;
  }

  private List<PartitionInfo> getAllPartitions(Consumer<K, V> consumer, Collection<String> topics) {
    final List<PartitionInfo> partitions = new ArrayList<>();
    for (final String topic : topics) {
      partitions.addAll(consumer.partitionsFor(topic));
    }
    return partitions;
  }

  Consumer<K, V> create(ConsumerConfig<K, V> consumerConfig) {
    return new KafkaConsumer<>(consumerConfig.props());
  }

  private void checkReasonablePollInterval(ConsumerConfig<K, V> consumerConfig) {
    final int defaultPollInterval = (int) Duration
        .ofMinutes(5)
        .toMillis();

    final int currentPollInterval = (int) consumerConfig
        .props()
        .getOrDefault(MAX_POLL_INTERVAL_MS_CONFIG, defaultPollInterval);

    final RetryPolicy retryPolicy = consumerConfig.retryPolicy();

    final long retryMaxWaitTime = retryPolicy
        .calcMaxTotalWaitTime()
        .toMillis();

    if (currentPollInterval < retryMaxWaitTime) {
      notifyNotRecommendedRetryPolicy(currentPollInterval, retryPolicy, retryMaxWaitTime);
    }
  }

  void notifyNotRecommendedRetryPolicy(int currentPollInterval, RetryPolicy retryPolicy, long retryMaxWaitTime) {
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
