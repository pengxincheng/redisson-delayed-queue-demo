package com.pxc.redissondelayedqueuedemo.delay;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author kiprince
 * rediss 延时队列操作类
 */
@Component
@Slf4j
public class RedisDelayQueue {

    @Resource
    private  RedissonClient redissonClient;

    private Map<String, RBlockingQueue<String>> blockingQueueMap = Maps.newConcurrentMap();

    private Map<String, RDelayedQueue<String>> delayedQueueMap = Maps.newConcurrentMap();

    /**
     * 销毁队列
     */
    void destroyQueue(String queueName) {
        RDelayedQueue<String> delayedQueue = delayedQueueMap.get(queueName);
        if (delayedQueue != null) {
            delayedQueue.destroy();
        }
    }

    /**
     * 获取阻塞队列
     *
     * @param queueName 队列名称
     * @return 队列
     */
    RBlockingQueue<String> getBlockingQueue(String queueName) {
        return blockingQueueMap.computeIfAbsent(queueName, k -> redissonClient.getBlockingQueue(queueName));
    }

    /**
     * 获取延时队列
     *
     * @param queueName 队列名称
     * @return 队列
     */
    RDelayedQueue<String> getDelayQueue(String queueName) {
        return delayedQueueMap.computeIfAbsent(queueName, k -> {
            RBlockingQueue<String> blockingQueue = blockingQueueMap.computeIfAbsent(queueName,
                    t -> redissonClient.getBlockingQueue(queueName));
            return redissonClient.getDelayedQueue(blockingQueue);
        });
    }

    /**
     * 向添加队列数据
     *
     * @param queueName 队列名称
     * @param t         DTO传输类
     * @param delay     时间数量
     * @param timeUnit  时间单位
     */
    public <T> void addQueue(String queueName, T t, long delay, TimeUnit timeUnit) {
        if (t == null) {
            return;
        }
        RDelayedQueue<String> delayedQueue = getDelayQueue(queueName);
        log.info("任务入队列，延时时间:{}({}), queueName:{}, task:{}", delay, timeUnit.name(), queueName, JSON.toJSONString(t));
        // 指定时间以后以后将消息发送到指定队列
        delayedQueue.offer(JSON.toJSONString(t), delay, timeUnit);
    }

    /**
     * 移除队列数据(返回被移除的队列数据)
     *
     * @param queueName 队列名称
     * @param t         DTO传输类
     * @param <T>       泛型
     */
    public <T extends BaseTask> List<T> removeQueueByIdentity(String queueName, T t, Class<T> classType) {
        RDelayedQueue<String> delayedQueue = getDelayQueue(queueName);
        log.info("任务移除队列，queueName:{}, taskId:{}", queueName, t.taskIdentity());
        List<String> dataList = delayedQueue.stream().filter(data -> {
            T k = JSON.parseObject(data, classType);
            if (k == null) {
                return false;
            }
            if (k.taskIdentity().equals(t.taskIdentity())) {
                log.info("匹配到需要删除的队列数据:{}", JSON.toJSONString(data));
                return true;
            }
            return false;
        }).collect(Collectors.toList());
        log.info("任务ID:{}匹配到删除的记录数:{}", t.taskIdentity(), dataList.size());
        delayedQueue.removeAllAsync(dataList);
        return dataList.stream().map(data -> JSON.parseObject(data, classType)).collect(Collectors.toList());
    }

}
