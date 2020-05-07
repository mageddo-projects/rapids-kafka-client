package templates;

import com.mageddo.kafka.client.ConsumerConfig;
import com.mageddo.kafka.client.ConsumingConfig;
import com.mageddo.kafka.client.DefaultConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class DefaultConsumerTemplates {
  static class MockedDefaultConsumer<K, V> extends DefaultConsumer<K, V> {
    @Override
    public void consume(
        Consumer<K, V> consumer,
        ConsumingConfig<K, V> consumingConfig,
        ConsumerRecords<K, V> records) {
    }

    @Override
    protected Consumer<K, V> consumer() {
      return null;
    }

    @Override
    protected ConsumerConfig<K, V> consumerConfig() {
      return null;
    }
  }
  public static<K, V> DefaultConsumer<K, V> build() {
    return new MockedDefaultConsumer<>();
  }
}
