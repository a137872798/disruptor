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

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Convenience class for handling the batching semantics of consuming entries from a {@link RingBuffer}
 * and delegating the available events to an {@link EventHandler}.
 * <p>
 * If the {@link EventHandler} also implements {@link LifecycleAware} it will be notified just after the thread
 * is started and just before the thread is shutdown.
 * 该对象基本可以看作是一个模板 定义了消费者的处理流程 (包含各种钩子的触发时机)  而能否消费数据的核心逻辑在 barrier 上
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public final class BatchEventProcessor<T>
    implements EventProcessor
{
    // 代表当前 Processor 的3种状态
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    /**
     * 默认属于 空闲状态
     */
    private final AtomicInteger running = new AtomicInteger(IDLE);
    /**
     * 该异常处理器会在捕获到 异常时 打印日志
     */
    private ExceptionHandler<? super T> exceptionHandler = new FatalExceptionHandler();
    /**
     * 数据提供者 只包含一个 get() 方法
     */
    private final DataProvider<T> dataProvider;
    /**
     * 与RingBuffer 交互的 屏障对象 核心方法是waitFor
     */
    private final SequenceBarrier sequenceBarrier;
    /**
     * 事件处理器
     */
    private final EventHandler<? super T> eventHandler;
    /**
     * 代表当前处理的序列  默认是-1
     */
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    /**
     * 超时处理器 包含 timeout(long sequence) 方法
     */
    private final TimeoutHandler timeoutHandler;
    /**
     * 当处理批数据时的钩子
     */
    private final BatchStartAware batchStartAware;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
     * the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param dataProvider    to which events are published.
     * @param sequenceBarrier on which it is waiting.
     * @param eventHandler    is the delegate to which events are dispatched.
     */
    public BatchEventProcessor(
        final DataProvider<T> dataProvider,    // 实际上就是 RingBuffer
        final SequenceBarrier sequenceBarrier,   // 屏障对象
        final EventHandler<? super T> eventHandler)  // 事件处理器
    {
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;

        // 初始化时 直接触发回调
        if (eventHandler instanceof SequenceReportingEventHandler)
        {
            ((SequenceReportingEventHandler<?>) eventHandler).setSequenceCallback(sequence);
        }

        // 当处理批任务时 出入长度触发回调
        batchStartAware =
            (eventHandler instanceof BatchStartAware) ? (BatchStartAware) eventHandler : null;
        // 针对超时时 传入sequence触发回调
        timeoutHandler =
            (eventHandler instanceof TimeoutHandler) ? (TimeoutHandler) eventHandler : null;
    }

    @Override
    public Sequence getSequence()
    {
        return sequence;
    }

    @Override
    public void halt()
    {
        // 修改标识为 暂停 同时触发 barrier 的 alert
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    /**
     * 非空闲状态就代表 代表正在运行
     * @return
     */
    @Override
    public boolean isRunning()
    {
        return running.get() != IDLE;
    }

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
     * 设置异常处理器
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler<? super T> exceptionHandler)
    {
        if (null == exceptionHandler)
        {
            throw new NullPointerException();
        }

        this.exceptionHandler = exceptionHandler;
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run()
    {
        // 确保CAS 成功
        if (running.compareAndSet(IDLE, RUNNING))
        {
            // 清除 之前halt 设置的 标识
            sequenceBarrier.clearAlert();

            // 触发钩子函数
            notifyStart();
            try
            {
                if (running.get() == RUNNING)
                {
                    // 处理事件
                    processEvents();
                }
            }
            finally
            {
                // 触发钩子
                notifyShutdown();
                running.set(IDLE);
            }
        }
        else
        {
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING)
            {
                throw new IllegalStateException("Thread is already running");
            }
            else
            {
                earlyExit();
            }
        }
    }

    /**
     * 处理事件的函数
     */
    private void processEvents()
    {
        T event = null;
        // 设置下个拉取的sequence
        long nextSequence = sequence.get() + 1L;

        while (true)
        {
            try
            {
                // 通过barrier 从RingBuffer 拉取数据
                // availableSequence 代表 最后可用的 序列 也就是 nextSequence 到 availableSequence 之间的数据都可以处理
                // 为什么可能一次会获取多个数据呢
                // 在ringbuffer 写入数据时 可能后申请到slot 的生产者先完成数据的创建 但是这时它不能提交 必须等待之前的slot提交
                // 这样的话就可能有很多事件囤积 这时就可以批量处理了
                final long availableSequence = sequenceBarrier.waitFor(nextSequence);
                if (batchStartAware != null && availableSequence >= nextSequence)
                {
                    // 触发钩子
                    batchStartAware.onBatchStart(availableSequence - nextSequence + 1);
                }

                while (nextSequence <= availableSequence)
                {
                    // 从ringbuffer 中拉取数据  看来首先通过barrier 阻塞获取某个 下标 等到被唤醒时 代表ringbuffer已经填入了数据
                    // 这时 不断拉取事件并 处理 追赶上生产者
                    event = dataProvider.get(nextSequence);
                    // 传入当前的 事件 和当前的偏移量 第三个参数 代表当前是否正在处理最后一个任务
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }

                // 更新偏移量
                sequence.set(availableSequence);
            }
            catch (final TimeoutException e)
            {
                // waitFor 可能会超时 这时就抛出超时异常
                notifyTimeout(sequence.get());
            }
            // 什么时候会出现这种情况???
            catch (final AlertException ex)
            {
                // 退出循环
                if (running.get() != RUNNING)
                {
                    break;
                }
            }
            // 遇到异常时 使用 exceptionHandler 去处理 同时 增加sequence 确保之后的数据能正常处理
            catch (final Throwable ex)
            {
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }

    /**
     * 提前退出  同时触发 start 和 shutdown 的钩子
     */
    private void earlyExit()
    {
        notifyStart();
        notifyShutdown();
    }

    /**
     * 当超时时触发
     * @param availableSequence
     */
    private void notifyTimeout(final long availableSequence)
    {
        try
        {
            if (timeoutHandler != null)
            {
                timeoutHandler.onTimeout(availableSequence);
            }
        }
        catch (Throwable e)
        {
            exceptionHandler.handleEventException(e, availableSequence, null);
        }
    }

    /**
     * Notifies the EventHandler when this processor is starting up
     * 如果 EventHandler 继承 LifecycleAware 接口 触发对应钩子
     */
    private void notifyStart()
    {
        if (eventHandler instanceof LifecycleAware)
        {
            try
            {
                ((LifecycleAware) eventHandler).onStart();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    /**
     * Notifies the EventHandler immediately prior to this processor shutting down
     */
    private void notifyShutdown()
    {
        if (eventHandler instanceof LifecycleAware)
        {
            try
            {
                ((LifecycleAware) eventHandler).onShutdown();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }
}