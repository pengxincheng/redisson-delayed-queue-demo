package com.pxc.redissondelayedqueuedemo.constant;

/**
 * @author pengxincheng
 * @date 2021/4/14 4:36 下午
 */
public class RedisKeys {
    /**
     * 业务1队列key
     */
    private static final String BIZ_ONE_QUEUE_KEY = "biz_one_queue";
    public static String getBizOneQueueKey(){
        return BIZ_ONE_QUEUE_KEY;
    }
    /**
     * 业务2队列key
     */
    private static final String BIZ_TWO_QUEUE_KEY = "biz_two_queue";
    public static String getBizTwoQueueKey(){
        return BIZ_TWO_QUEUE_KEY;
    }

}
