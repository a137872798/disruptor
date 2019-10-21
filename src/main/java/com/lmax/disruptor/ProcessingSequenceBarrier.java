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


/**
 * {@link SequenceBarrier} handed out for gating {@link EventProcessor}s on a cursor sequence and optional dependent {@link EventProcessor}(s),
 * using the given WaitStrategy.
 * 屏障对象 通过 Processor 内部调用 waitFor 阻塞并等待指定的序列写入数据
 */
final class ProcessingSequenceBarrier implements SequenceBarrier
{
    /**
     * 等待策略
     */
    private final WaitStrategy waitStrategy;

    /**
     * 代表该消费者 是否有其他前置的消费者
     */
    private final Sequence dependentSequence;
    /**
     * 是否处在禁止状态
     */
    private volatile boolean alerted = false;
    /**
     * 当前序列下标
     */
    private final Sequence cursorSequence;
    /**
     * 序列控制器
     */
    private final Sequencer sequencer;

    ProcessingSequenceBarrier(
        final Sequencer sequencer,        // 生产者序列控制器
        final WaitStrategy waitStrategy,   // 设置等待策略
        final Sequence cursorSequence,    // 生产者当前光标
        final Sequence[] dependentSequences)
    {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
        // 当没有依赖其他消费者时  依赖的消费者就是自身
        if (0 == dependentSequences.length)
        {
            dependentSequence = cursorSequence;
        }
        else
        {
            // 将一组序列包装成单个序列对象 只允许查询数据 不支持设置数据  该对象能够 直接返回当前依赖的消费者的最小偏移量
            dependentSequence = new FixedSequenceGroup(dependentSequences);
        }
    }

    /**
     * 阻塞 直到指定的某个序列的数值设置完成
     * @param sequence to wait for
     * @return
     * @throws AlertException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    @Override
    public long waitFor(final long sequence)
        throws AlertException, InterruptedException, TimeoutException
    {
        // 如果当前被设置成禁止状态 抛出异常
        checkAlert();

        // barrier 是通过阻塞策略 等待序列变化的
        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);

        // 返回的可用序列 能比 阻塞需求的序列还小吗???
        if (availableSequence < sequence)
        {
            return availableSequence;
        }

        // 通过sequencer 进行调配 返回一个合适的序列值
        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    /**
     * dependentSequence 默认情况 就是cursorSequence 如果有其他依赖的消费者 就是返回其他消费者的最小偏移量
     * @return
     */
    @Override
    public long getCursor()
    {
        return dependentSequence.get();
    }

    /**
     * 当前是否被禁止 如果被禁止调用 waitFor 会抛出异常
     * @return
     */
    @Override
    public boolean isAlerted()
    {
        return alerted;
    }

    /**
     * 从阻塞状态中解除
     */
    @Override
    public void alert()
    {
        alerted = true;
        // 唤醒所有 阻塞的线程
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * 当调用alert 方法后 必须调用该方法才能继续获取事件
     */
    @Override
    public void clearAlert()
    {
        alerted = false;
    }

    @Override
    public void checkAlert() throws AlertException
    {
        if (alerted)
        {
            throw AlertException.INSTANCE;
        }
    }
}