package templates;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import com.mageddo.kafka.client.ConsumerConfigDefault;
import com.mageddo.kafka.client.DefaultConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;

public class DefaultConsumerTemplates {
  public static class MockedDefaultConsumer<K, V> extends DefaultConsumer<K, V> {

    private String topic;
    private MockConsumer<K, V> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    public MockedDefaultConsumer() {
    }

    public MockedDefaultConsumer(String topic, List<PartitionInfo> partitionsInfo) {
      this.topic = topic;
      this.consumer().updatePartitions(topic, partitionsInfo);
      this.consumer().scheduleNopPollTask();
    }

    @Override
    public void consume(ConsumerRecords<K, V> records) {
    }

    @Override
    public MockConsumer<K, V> consumer() {
      return this.consumer;
    }

    @Override
    protected ConsumerConfigDefault<K, V> consumerConfig() {
      return ConsumerConfigDefault
          .<K, V>builder()
          .topics(this.topic)
          .callback((callbackContext, record) -> {})
          .pollInterval(Duration.ofMillis(500))
          .build();
    }

    @Override
    protected int getNumber() {
      return 0;
    }

  }

  public static <K, V> MockedDefaultConsumer<K, V> build() {
    return new MockedDefaultConsumer<>();
  }

  public static <K, V> MockedDefaultConsumer<K, V> build(String topic, PartitionInfo ... partitions) {
    return new MockedDefaultConsumer<>(topic, Arrays.asList(partitions));
  }
}
