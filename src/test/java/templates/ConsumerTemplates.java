package templates;

import lombok.experimental.UtilityClass;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;

@UtilityClass
public class ConsumerTemplates {
  public static <K, V> MockConsumer<K, V> build(String topic, List<PartitionInfo> partitions) {
    final MockConsumer<K, V> mockConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    mockConsumer.updatePartitions(topic, partitions);
    return mockConsumer;
  }
}
