package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author yaoyu  2019/4/18 - 13:10
 */


public class NewProducer {
    public static void main(String[] args) {
        // 1.配置属性值
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop101:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2.定义kafka生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 3.发送消息
        for (int i = 0; i < 50; i++) {
            // 发送到哪个topic,key, value
            producer.send(new ProducerRecord<String, String>("second", Integer.toString(i), "yaoyu-" + i));
        }

        // 4.关闭资源
        producer.close();

    }
}
