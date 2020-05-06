package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;

@Slf4j
public class ConsumerFactory<K, V> {

  private final BatchConsumer<K, V> batchConsumer = new BatchConsumer<>();
  private final RecordConsumer<K, V> recordConsumer = new RecordConsumer<>();

  public void consume(ConsumerConfig<K, V> consumerConfig) {
    this.checkReasonablePollInterval(consumerConfig);

    final KafkaConsumer<K, V> firstConsumer = create(consumerConfig);

    final List<Consumer<K, V>> consumers = new ArrayList<>();
    consumers.add(firstConsumer);
    for (int i = 0; i < consumerConfig.getConsumers() - 1; i++) {
      consumers.add(create(consumerConfig));
    }

    final List<PartitionInfo> topicPartitions = getAllPartitions(firstConsumer, consumerConfig.getTopics());
    final int partitionsByConsumer = Math.max(1, topicPartitions.size() / consumerConfig.getConsumers());
    final Deque<PartitionInfo> stack = new LinkedList<>(topicPartitions);
    for (final Consumer<K, V> consumer : consumers) {
      final List<PartitionInfo> partitions = popToList(stack, partitionsByConsumer);
      if(partitions.isEmpty()){
        log.info("status=no-partitions-left, there will be idle consumers");
        break;
      }
      this.start(consumer, consumerConfig, partitions);
    }
    log.info("status=consumers started");
  }

  private List<PartitionInfo> popToList(Deque<PartitionInfo> stack, int quantity) {
    final List<PartitionInfo> partitions = new ArrayList<>();
    for (int i = 0; i < Math.min(quantity, stack.size()); i++) {
      partitions.add(stack.pop());
    }
    return partitions;
  }

  private void start(
      final Consumer<K, V> consumer,
      final ConsumerConfig<K, V> consumerConfig,
      final List<PartitionInfo> partitions
  ) {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      log.info("status=consumer-starting, partitions-size={}, partitions={}", partitions.size(), partitions);
      consumer.assign(
          partitions
              .stream()
              .map(it -> new TopicPartition(it.topic(), it.partition()))
              .collect(Collectors.toList())
      );
      this.poll(consumer, consumerConfig);
    });
  }

  private List<PartitionInfo> getAllPartitions(KafkaConsumer<K, V> firstConsumer, Collection<String> topics) {
    final List<PartitionInfo> partitions = new ArrayList<>();
    for (final String topic : topics) {
      partitions.addAll(firstConsumer.partitionsFor(topic));
    }
    return partitions;
  }

  private KafkaConsumer<K, V> create(ConsumerConfig<K, V> consumerConfig) {
    return new KafkaConsumer<>(consumerConfig.getProps());
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    if (consumingConfig.getBatchCallback() == null && consumingConfig.getCallback() == null) {
      throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
    }
    final boolean batchConsuming = consumingConfig.getBatchCallback() != null;
    while (true) {
      try {
        final ConsumerRecords<K, V> records = consumer.poll(consumingConfig.getTimeout());
        if (log.isTraceEnabled()) {
          log.trace("status=polled, records={}", records.count());
        }
        this.consume(consumer, consumingConfig, records, batchConsuming);
      } catch (Exception e) {
        log.warn("status=consuming-error", e);
        if (batchConsuming) {
          consumingConfig
              .getBatchCallback()
              .accept(consumer, null, e);
        } else {
          consumingConfig
              .getCallback()
              .accept(consumer, null, e);
        }
      }
      try {
        TimeUnit.MILLISECONDS.sleep(
            consumingConfig
                .getInterval()
                .toMillis()
        );
      } catch (InterruptedException e) {
        Thread
            .currentThread()
            .interrupt();
        break;
      }
    }
  }

  private void consume(
      Consumer<K, V> consumer,
      ConsumingConfig<K, V> consumingConfig,
      ConsumerRecords<K, V> records,
      boolean batchConsuming
  ) {

    if (log.isTraceEnabled()) {
      log.trace("batch-consuming={}, records={}", batchConsuming, records.count());
    }
    if (batchConsuming) {
      this.batchConsumer.consume(consumer, consumingConfig, records);
    } else {
      this.recordConsumer.consume(consumer, consumingConfig, records);
    }

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
