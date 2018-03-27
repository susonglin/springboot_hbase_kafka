package com.muheda.notice.kafka;

import com.muheda.notice.entity.Notice;
import com.muheda.notice.service.NoticeService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @Author: Sorin
 * @Descriptions:
 * @Date: Created in 2018/3/23
 */
@Component("listener")
public class Listener {

    @Autowired
    private NoticeService noticeService;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = {"notice"})
    public void listen(ConsumerRecord<?, ?> record) {
        logger.info("kafka的key: " + record.key());
        logger.info("kafka的value: " + record.value().toString());

    }

    @KafkaListener(topics = {"notice2"})
    public void listen2(ConsumerRecord<?, ?> record) {
        logger.info("kafka2的key: " + record.key());
        logger.info("kafka2的value: " + record.value().toString());
    }

    /**
     * @Descripton: 发送通知
     * @Author: Sorin
     * @param record
     * @Date: 2018/3/26
     */
    @KafkaListener(topics = {"notice_send"})
    public void noticeSend(ConsumerRecord<?, ?> record){
        Notice notice = (Notice)record.value();
        // 发送通知
        noticeService.save(notice);
    }

    /**
     * @Descripton: 修改通知内容
     * @Author: Sorin
     * @param record
     * @Date: 2018/3/26
     */
    @KafkaListener(topics = {"notice_update"})
    public void noticeUpdate(ConsumerRecord<?, ?> record){
        try {
            Notice notice = (Notice)record.value();
            // 修改通知内容
            noticeService.update(notice);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("修改通知内容失败！");
        }
    }
}
