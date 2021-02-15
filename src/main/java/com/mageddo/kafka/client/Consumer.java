package com.mageddo.kafka.client;

public interface Consumer {
  <K, V> ConsumerConfig<K, V> config();
}
