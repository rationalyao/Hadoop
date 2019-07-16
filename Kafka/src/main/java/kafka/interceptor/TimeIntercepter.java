package kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author yaoyu  2019/4/19 - 9:21
 */


public class TimeIntercepter implements ProducerInterceptor<String, String> {
    //
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 拦截数据后，对数据加工处理
        // 在value值上增加时间戳的前缀
        return new ProducerRecord<String, String>(record.topic(), record.partition(), record.timestamp(),
                record.key(), System.currentTimeMillis() + "," + record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    // 获取配置信息,初始化操作
    @Override
    public void configure(Map<String, ?> map) {

    }
}
