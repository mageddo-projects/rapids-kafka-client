package com.mageddo.kafka.client;

import com.mageddo.kafka.client.ConsumerConfig;

public interface Consumer {
  ConsumerConfig<?, ?> config();
}
