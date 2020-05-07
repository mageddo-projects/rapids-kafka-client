package templates;

import lombok.experimental.UtilityClass;

import org.apache.kafka.common.PartitionInfo;

@UtilityClass
public class PartitionInfoTemplates {
  public static PartitionInfo build(String topic){
    return build(topic, 1);
  }
  public static PartitionInfo build(String topic, int partition){
    return new PartitionInfo(topic, partition, null, null, null);
  }
}
