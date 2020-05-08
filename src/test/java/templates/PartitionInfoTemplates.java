package templates;

import org.apache.kafka.common.PartitionInfo;

import lombok.experimental.UtilityClass;

@UtilityClass
public class PartitionInfoTemplates {
  public static PartitionInfo build(String topic){
    return build(topic, 1);
  }
  public static PartitionInfo build(String topic, int partition){
    return new PartitionInfo(topic, partition, null, null, null);
  }
}
