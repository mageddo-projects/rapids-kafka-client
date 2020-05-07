package templates;

import lombok.experimental.UtilityClass;

import org.apache.kafka.common.PartitionInfo;

@UtilityClass
public class PartitionInfoTemplates {
  public static PartitionInfo build(String topic){
    return new PartitionInfo(topic, 1, null, null, null);
  }
}
