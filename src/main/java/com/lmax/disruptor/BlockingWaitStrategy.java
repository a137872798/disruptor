/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import com.lmax.disruptor.util.ThreadHints;

/**
 * Blocking strategy that uses a lock and condition variable for {@link EventProcessor}s waiting on a barrier.
 * <p>
 * This strategy can be used when throughput and low-latency are not as important as CPU resource.
 * 基于 阻塞实现的 等待策略   如果使用自旋 会带来比较高的CPU 消耗 当允许放弃低延迟选择节省CPU资源时 可以选择该实现方式
 */
public final class BlockingWaitStrategy implements WaitStrategy
{
    /**
     * 看来是基于 JVM 内置锁实现
     */
    private final Object mutex = new Object();

    /**
     * 当每个 sequencer 调用waitFor 等待 指定的序列填充数据
     * @param sequence          to be waited on.            代表等待生产者的序列
     * @param cursorSequence                                代表当前消费者当前的偏移量
     * @param dependentSequence on which to wait.           代表该消费者 有依赖其他的消费者
     * @param barrier           the processor is waiting on.阻塞消费者线程的屏障对象
     * @return
     * @throws AlertException
     * @throws InterruptedException
     */
    @Override
    public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
        throws AlertException, InterruptedException
    {
        long availableSequence;

        // 首先如果消费者偏移量小于生产者 代表本次 尝试获取的 生产者偏移量有效

        if (cursorSequence.get() < sequence)
        {
            synchronized (mutex)
            {
                while (cursorSequence.get() < sequence)
                {
                    // 每次解除阻塞时判断消费者是否已经被禁止 被禁止的话抛出异常
                    barrier.checkAlert();
                    mutex.wait();
                }
            }
        }

        // 如果 该消费者 依赖于其他消费者 那么必须确保其他消费者消费完成
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            barrier.checkAlert();
            // 如果Thread 实现了 onSpinWait  通过目标方法实现自旋
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    /**
     * 外部线程 获取该锁并唤醒该对象
     */
    @Override
    public void signalAllWhenBlocking()
    {
        synchronized (mutex)
        {
            mutex.notifyAll();
        }
    }

    @Override
    public String toString()
    {
        return "BlockingWaitStrategy{" +
            "mutex=" + mutex +
            '}';
    }
}
