package com.example.redis1.app.util;

import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

@Component
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

    /**
     * <pre>XCLAIM</pre>
     * @param pendingMessage Redis pending message
     * @param consumerName Redis ConsumerName
     */
    public void claimStream(PendingMessage pendingMessage, String consumerName) {
        RedisAsyncCommands commands = (RedisAsyncCommands) this.redisTemplate
                .getConnectionFactory().getConnection().getNativeConnection();

        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .add(pendingMessage.getIdAsString())
                .add(pendingMessage.getGroupName())
                .add(consumerName)
                .add("20")
                .add(pendingMessage.getIdAsString());

        commands.dispatch(CommandType.XCLAIM, new StatusOutput(StringCodec.UTF8), args);
    }

    /**
     * <pre>RANGE</pre>
     * @param streamKey Redis StreamKey
     * @param startId start ID
     * @param endId end ID
     * @return message list
     */
    public List<MapRecord<String, Object, Object>> findStreamMessageByRange(String streamKey, String startId, String endId) {
        return this.redisTemplate.opsForStream().range(streamKey, Range.closed(startId, endId));
    }

    /**
     * <b>FIND ONE in RANGE</b>
     * @param streamKey Redis StreamKey
     * @param id ID
     * @return message
     */
    public MapRecord<String, Object, Object> findStreamMessageById(String streamKey, String id) {
        List<MapRecord<String, Object, Object>> mapRecordList = this.findStreamMessageByRange(streamKey, id, id);
        if (mapRecordList.isEmpty()) return null;
        return mapRecordList.get(0);
    }

    /**
     * <pre>XGROUP-ConsumerGroup</pre>
     * @param streamKey Redis StreamKey
     * @param consumerGroupName consumer group name
     */
    public void createStreamConsumerGroup(String streamKey, String consumerGroupName) {

        if (Boolean.FALSE.equals(this.redisTemplate.hasKey(streamKey))) {
            RedisAsyncCommands commands = (RedisAsyncCommands) this.redisTemplate
                    .getConnectionFactory().getConnection().getNativeConnection();

            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .add(CommandKeyword.CREATE)
                    .add(streamKey)
                    .add(consumerGroupName)
                    .add("0")
                    .add("MKSTREAM");

            commands.dispatch(CommandType.XGROUP, new StatusOutput(StringCodec.UTF8), args);
        } else {
            if (!isStreamConsumerGroupExist(streamKey, consumerGroupName)) {
                this.redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), consumerGroupName);
            }
        }
    }

    /**
     * <b>Redis ConsumerGroup 존재 여부 확인</b>
     * @param streamKey Redis StreamKey
     * @param consumerGroupName consumer group name
     * @return group 존재 여부 [true:존재, false:미존재]
     */
    public boolean isStreamConsumerGroupExist(String streamKey, String consumerGroupName) {
        Iterator<StreamInfo.XInfoGroup> iterator = this.redisTemplate
                .opsForStream().groups(streamKey).stream().iterator();

        while (iterator.hasNext()) {
            StreamInfo.XInfoGroup xInfoGroup = iterator.next();
            if (xInfoGroup.groupName().equals(consumerGroupName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * <b>기본 StreamMessageListenerContainer 생성 </b>
     * @return Redis StreamMessageListenerContainer
     */
    public StreamMessageListenerContainer createStreamMessageListenerContainer(){
        return StreamMessageListenerContainer.create(
                this.redisTemplate.getConnectionFactory(),
                StreamMessageListenerContainer
                        .StreamMessageListenerContainerOptions.builder()
                        .hashKeySerializer(new StringRedisSerializer())
                        .hashValueSerializer(new StringRedisSerializer())
                        .pollTimeout(Duration.ofMillis(20))
                        .build()
        );
    }

    public PendingMessages findStreamPendingMessages(String streamKey, String consumerGroupName, String consumerName){
        return this.redisTemplate.opsForStream()
                .pending(streamKey, Consumer.from(consumerGroupName, consumerName), Range.unbounded(), 100L);
    }

    public Object getRedisValue(String key, String field){
        return this.redisTemplate.opsForHash().get(key, field);
    }

    public long increaseRedisValue(String key, String field){
        return this.redisTemplate.opsForHash().increment(key, field, 1);
    }
}
