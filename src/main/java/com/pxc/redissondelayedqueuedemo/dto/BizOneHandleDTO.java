package com.pxc.redissondelayedqueuedemo.dto;

import com.pxc.redissondelayedqueuedemo.delay.BaseTask;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 业务场景1 处理
 * @author pengxincheng
 * @date 2021/4/14 4:23 下午
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BizOneHandleDTO extends BaseTask {

    private String bizId;

    private String bizName;


    @Override
    public String taskIdentity() {
        return bizId;
    }
}
