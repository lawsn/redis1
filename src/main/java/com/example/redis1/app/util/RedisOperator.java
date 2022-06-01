package com.example.redis1.app.util;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;

@RequiredArgsConstructor
public class RedisOperator {

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * <pre>XACK</pre>
     * @param consumerGroupName Redis GroupName
     * @param message Redis operation message
     */
    public void ackStream(String consumerGroupName, MapRecord<String, Object, Object> message) {
        this.redisTemplate.opsForStream().acknowledge(consumerGroupName, message);
    }
}
