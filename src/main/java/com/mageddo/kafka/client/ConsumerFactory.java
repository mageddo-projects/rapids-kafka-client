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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

@Slf4j
public class ConsumerFactory<K, V> implements AutoCloseable {

  private List<ThreadConsumer<K, V>> consumers = new ArrayList<>();
  private boolean closed;

  public static <K, V> ConsumerSupplier<K, V> defaultConsumerSupplier() {
    return config -> new KafkaConsumer<>(config.props());
  }

  public void consume(Consumers<K, V> consumerConfig) {
    if (consumerConfig.consumers() == Integer.MIN_VALUE) {
      log.info(
          "status=disabled-consumer, groupId={}, topics={}",
          consumerConfig.props()
              .get(GROUP_ID_CONFIG),
          consumerConfig.topics()
      );
      return;
    }
    this.checkReasonablePollInterval(consumerConfig);

    final Consumer<K, V> firstConsumer = create(consumerConfig);
    final Deque<PartitionInfo> partitions = new LinkedList<>(getAllPartitions(firstConsumer, consumerConfig.topics()));
    final int partitionsByConsumer = calcPartitionsByConsumer(consumerConfig, partitions.size());

    int startedConsumers = 1;
    for (int i = 0; i < consumerConfig.consumers() - 1; i++) {

      final List<PartitionInfo> consumerPartitions = popConsumerPartitions(partitions, partitionsByConsumer);
      if (consumerPartitions.isEmpty()) {
        log.info("status=no-consumerPartitions-left, there will be idle consumers");
        break;
      }
      final Consumer<K, V> consumer = create(consumerConfig);
      final ThreadConsumer<K, V> threadConsumer = getInstance(consumer, consumerConfig);
      threadConsumer.start(consumerPartitions);

      startedConsumers++;
    }
    final List<PartitionInfo> consumerPartitions = popConsumerPartitions(partitions, Integer.MAX_VALUE);
    if (consumerPartitions.isEmpty()) {
      log.debug("status=no-consumer-partitions-left-for-first-consumer");
      return;
    }
    final ThreadConsumer<K, V> threadConsumer = getInstance(firstConsumer, consumerConfig);
    threadConsumer.start(consumerPartitions);
    log.info("status={} consumers started", startedConsumers);
  }

  private int calcPartitionsByConsumer(Consumers<K, V> consumerConfig, int partitions) {
    return Math.max(1, partitions / consumerConfig.consumers());
  }

  ThreadConsumer<K, V> getInstance(Consumer<K, V> consumer, Consumers<K, V> consumerConfig) {
    if (consumerConfig.batchCallback() != null) {
      return bindInstance(new BatchConsumer<>(consumer, consumerConfig));
    }
    return bindInstance(new RecordConsumer<>(consumer, consumerConfig));
  }

  private ThreadConsumer<K, V> bindInstance(ThreadConsumer<K, V> consumer) {
    this.consumers.add(consumer);
    return consumer;
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

  Consumer<K, V> create(Consumers<K, V> consumerConfig) {
    return consumerConfig.consumerSupplier()
        .get(consumerConfig);
  }

  private void checkReasonablePollInterval(Consumers<K, V> consumerConfig) {
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

  @Override
  public void close() throws Exception {
    if(closed){
      return ;
    }
    for (ThreadConsumer<K, V> consumer : this.consumers) {
      consumer.close();
    }
    this.closed = true;
  }

  /**
   * wait until all consumers threads termination
   */
  @SneakyThrows
  public ConsumerFactory<K, V> waitFor() {
    Thread.currentThread().join();
    return this;
  }
}
