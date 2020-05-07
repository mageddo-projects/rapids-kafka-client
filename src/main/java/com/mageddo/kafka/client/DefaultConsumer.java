package com.mageddo.kafka.client;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public abstract class DefaultConsumer<K, V> implements ThreadConsumer<K, V> {

  private AtomicBoolean started = new AtomicBoolean();

  public abstract void consume(
      Consumer<K, V> consumer,
      ConsumingConfig<K, V> consumingConfig,
      ConsumerRecords<K, V> records
  );

  protected abstract Consumer<K, V> consumer();
  protected abstract ConsumerConfig<K, V> consumerConfig();

  @Override
  public void start(final List<PartitionInfo> partitions) {
    if(started.get()){
      log.warn("status=already-started, partitions={}", partitions);
      return ;
    }
    final Consumer<K, V> consumer = consumer();
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(() -> {
      log.info("status=consumer-starting, partitions-size={}, partitions={}", partitions.size(), partitions);
      consumer.assign(
          partitions
              .stream()
              .map(it -> new TopicPartition(it.topic(), it.partition()))
              .collect(Collectors.toList())
      );
      this.poll(consumer, consumerConfig());
    });
    started.set(true);
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    if (consumingConfig.batchCallback() == null && consumingConfig.callback() == null) {
      throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
    }
    final boolean batchConsuming = consumingConfig.batchCallback() != null;
    while (true) {
      try {
        final ConsumerRecords<K, V> records = consumer.poll(consumingConfig.timeout());
        if (log.isTraceEnabled()) {
          log.trace("status=polled, records={}", records.count());
        }
        this.consume(consumer, consumingConfig, records);
      } catch (Exception e) {
        log.warn("status=consuming-error", e);
        if (batchConsuming) {
          consumingConfig
              .batchCallback()
              .accept(consumer, null, e);
        } else {
          consumingConfig
              .callback()
              .accept(consumer, null, e);
        }
      }
      try {
        TimeUnit.MILLISECONDS.sleep(
            consumingConfig
                .interval()
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

}
