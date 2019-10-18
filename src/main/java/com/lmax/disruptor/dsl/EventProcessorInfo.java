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
package com.lmax.disruptor.dsl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.Executor;

/**
 * <p>Wrapper class to tie together a particular event processing stage</p>
 * <p>
 * <p>Tracks the event processor instance, the event handler instance, and sequence barrier which the stage is attached to.</p>
 * 内部存放3个重要对象   这里特意没有让 EventProcessor 直接暴露 eventHandler 和 barrier 的获取方法***
 * 明确划分了职能
 * @param <T> the type of the configured {@link EventHandler}
 */
class EventProcessorInfo<T> implements ConsumerInfo
{
    private final EventProcessor eventprocessor;    // 事件处理模板   实际上该对象内部已经包含下面2个对象了
    private final EventHandler<? super T> handler;  // 事件处理器
    private final SequenceBarrier barrier;           // 该processor 对应的栅栏
    /**
     * 默认情况 生产者追上了消费者
     */
    private boolean endOfChain = true;

    EventProcessorInfo(
        final EventProcessor eventprocessor, final EventHandler<? super T> handler, final SequenceBarrier barrier)
    {
        this.eventprocessor = eventprocessor;
        this.handler = handler;
        this.barrier = barrier;
    }

    public EventProcessor getEventProcessor()
    {
        return eventprocessor;
    }

    @Override
    public Sequence[] getSequences()
    {
        // 这里只是返回单个 Sequence 为啥要封装成数组 同样的在设置 Disruptor 时 也使用一个数组来创建barrier 对象 即使数组只有一个元素
        return new Sequence[]{eventprocessor.getSequence()};
    }

    public EventHandler<? super T> getHandler()
    {
        return handler;
    }

    @Override
    public SequenceBarrier getBarrier()
    {
        return barrier;
    }

    @Override
    public boolean isEndOfChain()
    {
        return endOfChain;
    }

    /**
     * 使用执行的executor 来执行 processor  (Executor 会专门分发线程来执行任务)
     * @param executor
     */
    @Override
    public void start(final Executor executor)
    {
        executor.execute(eventprocessor);
    }

    /**
     * 对外开放 halt 接口 内部委托给 eventProcessor 来实现
     */
    @Override
    public void halt()
    {
        eventprocessor.halt();
    }

    /**
     * 代表正在等待barrier 拉取数据  这时设置endOfChain 为false
     */
    @Override
    public void markAsUsedInBarrier()
    {
        endOfChain = false;
    }

    @Override
    public boolean isRunning()
    {
        return eventprocessor.isRunning();
    }
}
