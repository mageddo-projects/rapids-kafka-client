package templates;

import java.util.Arrays;
import java.util.List;

import com.mageddo.kafka.client.Consumers;
import com.mageddo.kafka.client.DefaultConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;

public class DefaultConsumerTemplates {
  public static class MockedDefaultConsumer<K, V> extends DefaultConsumer<K, V> {

    private MockConsumer<K, V> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    public MockedDefaultConsumer() {
    }

    public MockedDefaultConsumer(String topic, List<PartitionInfo> partitionsInfo) {
      this.consumer().updatePartitions(topic, partitionsInfo);
      this.consumer().scheduleNopPollTask();
    }

    @Override
    public void consume(
        ConsumerRecords<K, V> records) {
    }

    @Override
    public MockConsumer<K, V> consumer() {
      return this.consumer;
    }

    @Override
    protected Consumers<K, V> consumerConfig() {
      return null;
    }

    @Override
    protected void onErrorCallback(Exception e) {

    }
  }

  public static <K, V> MockedDefaultConsumer<K, V> build() {
    return new MockedDefaultConsumer<>();
  }

  public static <K, V> MockedDefaultConsumer<K, V> build(String topic, PartitionInfo ... partitions) {
    return new MockedDefaultConsumer<>(topic, Arrays.asList(partitions));
  }
}
