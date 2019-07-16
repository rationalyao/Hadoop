package kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author yaoyu  2019/4/18 - 14:03
 */


public class CustomPartition implements Partitioner {

    // 定制分区
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    // 关闭处理
    @Override
    public void close() {

    }

    // 初始化
    @Override
    public void configure(Map<String, ?> map) {

    }
}
