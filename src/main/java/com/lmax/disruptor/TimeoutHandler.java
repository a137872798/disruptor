package com.lmax.disruptor;

/**
 * 超时处理器
 */
public interface TimeoutHandler
{
    void onTimeout(long sequence) throws Exception;
}
