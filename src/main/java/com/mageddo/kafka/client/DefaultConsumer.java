package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DefaultConsumer<K, V> implements ThreadConsumer<K, V>, AutoCloseable {

  private AtomicBoolean started = new AtomicBoolean();

  private Future<?> future;
  private ExecutorService executor;

  protected abstract void consume(ConsumerRecords<K, V> records);
  protected abstract Consumer<K, V> consumer();
  protected abstract Consumers<K, V> consumerConfig();

  @Override
  public void start() {
    if(started.get()){
      log.warn("status=already-started");
      return ;
    }
    final Consumer<K, V> consumer = consumer();
    this.executor = Executors.newSingleThreadExecutor();
    this.future = executor.submit(() -> {
      log.info("status=consumer-starting");
      consumer.subscribe(consumerConfig().topics());
      this.poll(consumer, consumerConfig());
    });
    started.set(true);
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    if (consumingConfig.batchCallback() == null && consumingConfig.callback() == null) {
      throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
    }
    while (!Thread.currentThread().isInterrupted()) {
      final ConsumerRecords<K, V> records = consumer.poll(consumingConfig.pollTimeout());
      if (log.isTraceEnabled()) {
        log.trace("status=polled, records={}", records.count());
      }
      this.consume(records);
      if(!Duration.ZERO.equals(consumingConfig.pollInterval())){
        this.sleep(consumingConfig.pollInterval());
      }
    }
    consumer.unsubscribe();
    consumer.close();
    log.info("status=consumer-stopped, {}", Thread.currentThread().getName());
  }

  /**
   * Sleep for some duration
   */
  protected void sleep(Duration timeout) {
    try {
      TimeUnit.MILLISECONDS.sleep(timeout.toMillis());
    } catch (InterruptedException e) {
      Thread
          .currentThread()
          .interrupt();
    }
  }

  void commitSyncRecord(Consumer<K, V> consumer, ConsumerRecord<K, V> record) {
    consumer.commitSync(Collections.singletonMap(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset())
    ));
  }

  void doRecoverWhenAvailable(RecoverContext<K, V> ctx, RecoverCallback<K, V> recoverCallback) {
    if (recoverCallback != null) {
      recoverCallback.recover(ctx);
      commitSyncRecord(ctx.consumer(), ctx.record());
    } else {
      log.warn("status=no recover callback was specified");
    }
  }

  @Override
  public void close() {
    log.info("status=consumer-stopping, {}", Thread.currentThread().getName());
    this.future.cancel(true);
    this.executor.shutdown();
  }

  protected ExecutorService getExecutor() {
    return executor;
  }
}
