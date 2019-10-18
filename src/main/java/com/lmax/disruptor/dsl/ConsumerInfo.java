package com.lmax.disruptor.dsl;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.Executor;

/**
 * 代表消费者信息
 */
interface ConsumerInfo
{
    Sequence[] getSequences();

    /**
     * 获取该消费者相关的 序列屏障  消费者通过它来访问 RingBuffer
     * @return
     */
    SequenceBarrier getBarrier();

    /**
     * 判断是否追赶上生产者
     * @return
     */
    boolean isEndOfChain();

    void start(Executor executor);

    void halt();

    void markAsUsedInBarrier();

    boolean isRunning();
}
