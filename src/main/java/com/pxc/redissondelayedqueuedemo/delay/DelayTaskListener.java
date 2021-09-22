package com.pxc.redissondelayedqueuedemo.delay;

/**
 * 延时任务监听接口
 *
 * @author kiprince
 */
public interface DelayTaskListener<T extends BaseTask> {

    /**
     * 延时调用方法
     *
     * @param t 任务元素
     */
    void invoke(T t);

}
