package com.example.redis1.app.scheduler;

import com.example.redis1.app.util.RedisOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@EnableScheduling
@Component
@RequiredArgsConstructor
public class PendingMessageScheduler implements InitializingBean {

    private String streamKey;
    private String consumerGroupName;
    private String consumerName;
    private final RedisOperator redisOperator;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.streamKey = "mystream";
        this.consumerGroupName = "consumerGroupName";
        this.consumerName = "consumerName";
    }

    @Scheduled(fixedRate = 10000)
    public void processPendingMessage() {
        log.info("echo times");
        PendingMessages pendingMessages = this.redisOperator
                .findStreamPendingMessages(streamKey, consumerGroupName, consumerName);

        for (PendingMessage pendingMessage : pendingMessages) {
            this.redisOperator.claimStream(pendingMessage, consumerName);
            try {
                MapRecord<String, Object, Object> messageToProcess = this.redisOperator
                        .findStreamMessageById(this.streamKey, pendingMessage.getIdAsString());
                if (messageToProcess == null) {
                    log.info("not exist message");
                } else {
                    int errorCount = (int) this.redisOperator
                            .getRedisValue("errorCount", pendingMessage.getIdAsString());

                    if (errorCount >= 5) {
                        log.info("exceed maximum retry count");
                    } else if (pendingMessage.getTotalDeliveryCount() >= 2) {
                        log.info("exceed maximum delivery count");
                    } else {
                        log.info("do something");
                    }

                    this.redisOperator.ackStream(consumerGroupName, messageToProcess);
                }
            } catch (Exception e) {
                this.redisOperator.increaseRedisValue("errorCount", pendingMessage.getIdAsString());
            }
        }
    }
}
