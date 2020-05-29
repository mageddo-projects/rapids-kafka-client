package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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

    for (int i = 0; i < consumerConfig.consumers() - 1; i++) {
      final ThreadConsumer<K, V> consumer = getInstance(create(consumerConfig), consumerConfig);
      consumer.start();
    }
    log.info("status={} consumers started", consumerConfig.consumers());
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
    if(this.closed){
      log.warn("status=already-closed");
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
