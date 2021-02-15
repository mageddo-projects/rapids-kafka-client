package com.mageddo.kafka.client.util;

import com.mageddo.kafka.client.ConsumerConfig;

public interface Consumer {
  ConsumerConfig<?, ?> config();
}
