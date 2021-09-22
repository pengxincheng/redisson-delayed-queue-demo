package com.pxc.redissondelayedqueuedemo.listener;

import com.alibaba.fastjson.JSON;
import com.pxc.redissondelayedqueuedemo.delay.DelayTaskListener;
import com.pxc.redissondelayedqueuedemo.dto.BizOneHandleDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author pengxincheng
 * @date 2021/4/14 4:32 下午
 */
@Slf4j
@Component
public class BizOneListener implements DelayTaskListener<BizOneHandleDTO> {

    @Override
    public void invoke(BizOneHandleDTO bizOneHandleDTO) {
        // 业务逻辑处理
        log.info("bizOne 逻辑", JSON.toJSONString(bizOneHandleDTO));
    }
}
