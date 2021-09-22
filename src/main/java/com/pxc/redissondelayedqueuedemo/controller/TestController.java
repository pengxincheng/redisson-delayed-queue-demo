package com.pxc.redissondelayedqueuedemo.controller;

import com.pxc.redissondelayedqueuedemo.constant.RedisKeys;
import com.pxc.redissondelayedqueuedemo.delay.RedisDelayQueue;
import com.pxc.redissondelayedqueuedemo.dto.BizOneHandleDTO;
import com.pxc.redissondelayedqueuedemo.dto.BizTwoHandleDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @author pengxincheng
 * @date 2021/4/14 4:25 下午
 */
@RestController
@RequestMapping("test")
public class TestController {

    @Resource
    private RedisDelayQueue redisDelayQueue;

    @GetMapping("bizOne")
    public String bizOneTest(@RequestParam("bizId") String bizId, @RequestParam("delayTime") Long delayTime) {

        BizOneHandleDTO bizOneHandle = BizOneHandleDTO.builder()
                .bizId(bizId)
                .bizName("bizOne test")
                .build();

        redisDelayQueue.addQueue(RedisKeys.getBizOneQueueKey(),bizOneHandle,delayTime, TimeUnit.SECONDS);

        return "success";
    }


    @GetMapping("bizTwo")
    public String bizTwoTest(@RequestParam("bizId") String bizId, @RequestParam("delayTime") Long delayTime){
        BizTwoHandleDTO bizTwoHandle = BizTwoHandleDTO.builder()
                .bizId(bizId)
                .bizName("bizTwo test")
                .build();
        redisDelayQueue.addQueue(RedisKeys.getBizTwoQueueKey(),bizTwoHandle,delayTime, TimeUnit.SECONDS);
        return "success";
    }

}
