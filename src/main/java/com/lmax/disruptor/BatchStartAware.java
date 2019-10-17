package com.lmax.disruptor;

/**
 * 感知对象 当进行批任务处理时触发 参数是 批量数据长度
 */
public interface BatchStartAware
{
    void onBatchStart(long batchSize);
}
