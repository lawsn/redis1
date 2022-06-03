package com.example.redis1.app.consumer;

import com.example.redis1.app.util.RedisOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedisStreamConsumer implements StreamListener<String, MapRecord<String, Object, Object>>
        , InitializingBean, DisposableBean {

    private StreamMessageListenerContainer<String, MapRecord<String, Object, Object>> listenerContainer;
    private Subscription subscription;
    private String streamKey;
    private String consumerGroupName;
    private String consumerName;

    // 위에 구현한 Redis Stream 에 필요한 기본 Command 를 구현한 Component
    private final RedisOperator redisOperator;

    @Override
    public void destroy() throws Exception {
        if (this.subscription != null) {
            this.subscription.cancel();
        }

        if (this.listenerContainer != null) {
            this.listenerContainer.stop();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.streamKey = "mystream";
        this.consumerGroupName = "consumerGroupName";
        this.consumerName = "consumerName";

        this.redisOperator.createStreamConsumerGroup(streamKey, consumerGroupName);

        this.listenerContainer = this.redisOperator.createStreamMessageListenerContainer();

        this.subscription = this.listenerContainer.receive(
                Consumer.from(this.consumerGroupName, consumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                this
        );
    }

    @Override
    public void onMessage(MapRecord<String, Object, Object> message) {
        this.redisOperator.ackStream(streamKey, message);
    }
}
