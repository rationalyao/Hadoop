package kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author yaoyu  2019/4/19 - 10:04
 */


public class ContentInterceptor implements ProducerInterceptor<String, String> {

    private int successCount = 0;
    private int errorCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 直接返回输入的对象
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 发送回调接口
        if ( e== null) { // 发送成功
            successCount++;
        }else {  // 发送失败
            errorCount++;
        }
    }

    @Override
    public void close() {
        // 最终统计输出
        System.err.println("发送成功次数：" + successCount + "发送失败次数" + errorCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
