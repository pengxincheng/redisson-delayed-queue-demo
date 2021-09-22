package com.pxc.redissondelayedqueuedemo.config;

import com.pxc.redissondelayedqueuedemo.constant.RedisKeys;
import com.pxc.redissondelayedqueuedemo.delay.RedisDelayEngine;
import com.pxc.redissondelayedqueuedemo.delay.RedisDelayQueue;
import com.pxc.redissondelayedqueuedemo.dto.BizOneHandleDTO;
import com.pxc.redissondelayedqueuedemo.dto.BizTwoHandleDTO;
import com.pxc.redissondelayedqueuedemo.listener.BizOneListener;
import com.pxc.redissondelayedqueuedemo.listener.BizTwoListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author pengxincheng
 * @date 2021/4/14 5:11 下午
 */
@Slf4j
@Configuration
public class RedisDelayConfig {

    @Resource
    private RedisDelayQueue redisDelayQueue;
    @Resource
    private BizOneListener bizOneListener;
    @Resource
    private BizTwoListener bizTwoListener;

    @Bean(value = "bizOneDelayEngine", destroyMethod = "destroy")
    public RedisDelayEngine<BizOneHandleDTO> taskStartDelayEngine() {
        log.info("BizOne开始延时任务引擎初始化");
        RedisDelayEngine<BizOneHandleDTO> redisDelayEngine = new RedisDelayEngine<>(redisDelayQueue,
                RedisKeys.getBizOneQueueKey(), bizOneListener, BizOneHandleDTO.class);
        redisDelayEngine.setTryTimes(1);
        return redisDelayEngine;
    }

    @Bean(value = "taskNoticeDelayEngine", destroyMethod = "destroy")
    public RedisDelayEngine<BizTwoHandleDTO> taskNoticeDelayEngine() {
        log.info("BizTwo提醒通知延时任务引擎初始化");
        RedisDelayEngine<BizTwoHandleDTO> redisDelayEngine = new RedisDelayEngine<>(redisDelayQueue,
               RedisKeys.getBizTwoQueueKey(), bizTwoListener, BizTwoHandleDTO.class);

        // 异常重试次数
        redisDelayEngine.setTryTimes(3);
        redisDelayEngine.setReTryDelay(3000L);
        return redisDelayEngine;
    }
}
