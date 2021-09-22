package com.pxc.redissondelayedqueuedemo.delay;

import lombok.Data;

/**
 * 任务执行对象基础类
 * @author kiprince
 */
@Data
public abstract class BaseTask {

    /**
     * 任务执行错误次数
     */
    private int errorTimes = 1;

    /**
     * 任务唯一标识
     * @return 任务唯一标识
     */
    public abstract String taskIdentity();

}
