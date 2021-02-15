package com.mageddo.kafka.client.util;

import com.mageddo.kafka.client.ConsumerConfigDefault;

public interface Consumer {
  ConsumerConfigDefault<?, ?> config();
}
