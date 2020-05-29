package com.mageddo.kafka.client;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DefaultConsumer<K, V> implements ThreadConsumer<K, V>, AutoCloseable {

  private AtomicBoolean started = new AtomicBoolean();
  private Thread executor;
  private boolean closed;
  private boolean stopped;

  protected abstract void consume(ConsumerRecords<K, V> records);

  protected abstract Consumer<K, V> consumer();

  protected abstract Consumers<K, V> consumerConfig();

  @Override
  public void start() {
    if (started.get()) {
      log.warn("status=already-started");
      return;
    }
    final Consumer<K, V> consumer = consumer();
    this.executor = new Thread(() -> {
      this.poll(consumer, consumerConfig());
    });
    this.executor.start();
    started.set(true);
  }

  public void poll(Consumer<K, V> consumer, ConsumingConfig<K, V> consumingConfig) {
    log.info("status=consumer-starting, id={}", this.id());
    consumer.subscribe(consumerConfig().topics());
    if (consumingConfig.batchCallback() == null && consumingConfig.callback() == null) {
      throw new IllegalArgumentException("You should inform BatchCallback Or Callback");
    }
    try {
      while (this.mustRun()) {
        final ConsumerRecords<K, V> records = consumer.poll(consumingConfig.pollTimeout());
        if (log.isTraceEnabled()) {
          log.trace("status=polled, records={}", records.count());
        }
        this.consume(records);
        if (!Duration.ZERO.equals(consumingConfig.pollInterval())) {
          this.sleep(consumingConfig.pollInterval());
        }
      }
    } catch (Exception e){
      log.warn("consumer-failed, id={}", this.id(), e);
    } finally {
      try {
        consumer.close();
      } catch (InterruptException e){}
    }
    log.debug("status=consumer-stopped, id={}", this.id());
    this.stopped = true;
  }

  protected boolean mustRun() {
    return !this.isClosed() && !Thread.currentThread().isInterrupted();
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

  @SneakyThrows
  @Override
  public void close() {
    if(this.isClosed()){
      log.warn("status=already-closed, id={}", this.id());
      return ;
    }
    log.debug("status=closing, id={}", this.id());
    this.closed = true;
    while (!this.isStopped()) {
      Thread.sleep(this.consumerConfig()
          .pollInterval()
          .toMillis());
    }
  }

  @Override
  public String id() {
    return String.format("%d-%s", this.executor.getId(), this.executor.getName());
  }

  public boolean isClosed() {
    return closed;
  }

  public boolean isStopped() {
    return stopped;
  }
}
