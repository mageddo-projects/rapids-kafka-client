package templates;

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerTemplates {
  public static <K, V> MockConsumer<K, V> buildWithOnePartition(String topic) {
    return build(
        topic,
        Collections.singletonList(PartitionInfoTemplates.build(topic))
    );
  }
  public static <K, V> MockConsumer<K, V> build(String topic, List<PartitionInfo> partitions) {
    final MockConsumer<K, V> mockConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    mockConsumer.updatePartitions(topic, partitions);
    return mockConsumer;
  }
}
