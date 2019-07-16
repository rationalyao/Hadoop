package kafka.interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author yaoyu  2019/4/19 - 10:13
 */


public class InterceptorProducer {
    public static void main(String[] args) {
        // 1.设置配置信息
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

        // 2.构建拦截器链
        ArrayList<Object> interceptors = new ArrayList<>();
        interceptors.add("kafka.interceptor.TimeIntercepter.java");
        interceptors.add("kafka.interceptor.ContentInterceptor.java");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 3.创建一个发送者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "first";
        // 4.发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topic, "message" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.err.println("haha wolaile");
                }
            });
        }

        // 5.关闭资源
        producer.close();
    }
}
