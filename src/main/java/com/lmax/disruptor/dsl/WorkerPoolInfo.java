package com.lmax.disruptor.dsl;

import com.lmax.disruptor.*;

import java.util.concurrent.Executor;

/**
 * 工作池相关信息 与 EventProcessorInfo 同级别
 * @param <T>
 */
class WorkerPoolInfo<T> implements ConsumerInfo
{
    /**
     * 工作池对象
     */
    private final WorkerPool<T> workerPool;
    /**
     * 栅栏
     */
    private final SequenceBarrier sequenceBarrier;
    /**
     * 是否处理完事件
     */
    private boolean endOfChain = true;

    WorkerPoolInfo(final WorkerPool<T> workerPool, final SequenceBarrier sequenceBarrier)
    {
        this.workerPool = workerPool;
        this.sequenceBarrier = sequenceBarrier;
    }

    /**
     * 从工作池中获取 序列 注意这里返回明确是一个数组 而 EventProcessor 只是单个Sequence 对象
     * @return
     */
    @Override
    public Sequence[] getSequences()
    {
        return workerPool.getWorkerSequences();
    }

    @Override
    public SequenceBarrier getBarrier()
    {
        return sequenceBarrier;
    }

    @Override
    public boolean isEndOfChain()
    {
        return endOfChain;
    }

    /**
     * 使用工作池来执行任务
     * @param executor
     */
    @Override
    public void start(Executor executor)
    {
        workerPool.start(executor);
    }

    @Override
    public void halt()
    {
        workerPool.halt();
    }

    @Override
    public void markAsUsedInBarrier()
    {
        endOfChain = false;
    }

    @Override
    public boolean isRunning()
    {
        return workerPool.isRunning();
    }
}
