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

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

import com.lmax.disruptor.util.Util;


/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> * Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.</p>
 * 多生产者的 序列调控器
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
    private static final Unsafe UNSAFE = Util.getUnsafe();
    /**
     * 代表分配的某个 数组的基础地址偏移量
     */
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);
    /**
     * 基于BASE 的偏移量 对应数组下标
     */
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

    /**
     * 代表当前缓存的序列
     */
    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    /**
     * 该对象映射到环形缓冲区的每个槽的 状态 初始状态下都是-1
     */
    private final int[] availableBuffer;
    /**
     * 下标掩码
     */
    private final int indexMask;
    /**
     * 代表该值是2的多少次幂
     */
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     * 创建一个多生产者的 序列控制器 （该对象用来驱使ringBuffer 的下标移动）
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        // 维护了一个数组  对应环形缓冲区的大小
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        // 代表该值是2的 多少次幂
        indexShift = Util.log2(bufferSize);
        // 使用-1 从后往前填充buffer
        initialiseAvailableBuffer();
    }


    /**
     * 判断是否还有可用的容量  主要是 与尚未消费的偏移量做比较
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    /**
     *
     * @param gatingSequences 代表一组消费者当前消费的序列
     * @param requiredCapacity   请求获取的长度
     * @param cursorValue    当前指针
     * @return
     */
    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
    {
        // 首先 cursorValue 肯定是小于 bufferSize 的如果加上 需要的偏移量 超过了 bufferSize 代表需要至少下轮 甚至可能更多轮 如果是负数 代表还在本轮内
        // ringBuffer 是一个使用 轮算法的数组
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        // 该缓存的序列 可能要理解为 多个生产者 每个生产者 维护自己的序列  该值就是未消费的最小的偏移量
        long cachedGatingSequence = gatingSequenceCache.get();

        // 这里大体的意思就是 必须确保 写入的位置已经被 消费
        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            // 获取这组门中最小的偏移量
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            // 代表要到下一轮 且该指针超过了未消费的偏移量 就必然无法分配
            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * 修改当前序列为指定的值
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * 获取下一个偏移量
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * 获取下几个序列值
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n)
    {
        if (n < 1 || n > bufferSize)
        {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        long current;
        long next;

        do
        {
            // 获取当前指针
            current = cursor.get();
            // 获取目标指针
            next = current + n;

            // 这里有严格约束  每次请求的大小不能超过一个 bufferSize 所以下面的判断能得到保证
            long wrapPoint = next - bufferSize;
            // 获取缓存的 最小的未消费sequence
            long cachedGatingSequence = gatingSequenceCache.get();

            // cachedGatingSequence 存在2种情况
            // 第一种 在 current的左边 0 --- cachedGatingSequence ---- current --- tail
            // 第二种 在current 的右边  0 ------ current ----- cachedGatingSequence ----- tail
            // wrapPoint 从0 数到一个指定的位置 小于 cachedGatingSequence 必然能分配成功 所以大于0 需要重新做判断
            // 而当小于0 的情况 第一种是肯定能分配的
            // 第二种情况 小于0 应该是不一定的   TODO 这里还需要更多的信息才好梳理
            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
            {
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);

                // 代表发生追尾 无法分配 通过LockSupport 配合 自旋
                if (wrapPoint > gatingSequence)
                {
                    LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
                    continue;
                }

                // 设置最新的 未消费偏移量
                gatingSequenceCache.set(gatingSequence);
            }
            // 更新光标
            else if (cursor.compareAndSet(current, next))
            {
                break;
            }
        }
        while (true);

        return next;
    }

    /**
     * 尝试修改 如果 没有足够空间抛出异常 如果CAS 失败 则进行重试
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do
        {
            current = cursor.get();
            next = current + n;

            if (!hasAvailableCapacity(gatingSequences, n, current))
            {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        // 代表目前以消费的 偏移量
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        // 当前生产者要填充的下标
        long produced = cursor.get();
        // (produced - consumed) 代表整个 buffer 中 没来得及消费 的部分 那么剩余的就是可填充的部分
        return getBufferSize() - (produced - consumed);
    }

    /**
     * 从后往前使用-1填充buffer
     */
    private void initialiseAvailableBuffer()
    {
        for (int i = availableBuffer.length - 1; i != 0; i--)
        {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * 开始发布事件
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        // 代表需要占用该空间
        setAvailable(sequence);
        // 唤醒所有阻塞对象 也就是 通知对应的消费者有新事件了
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * 占用一组空间
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * The below methods work on the availableBuffer flag.
     * <p>
     * The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     * <p>
     * --  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).这里明确声明了一次申请的大小不允许超过一个 bufferSize
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     * 设置某个 下标是否可用
     */
    private void setAvailable(final long sequence)
    {
        // calculateIndex(sequence) 根据掩码 计算 序列对应的 ringbuffer 的下标
        // calculateAvailabilityFlag 为 sequence * ringBuffer的大小
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    /**
     * 设置 buffer 的标识
     * @param index
     * @param flag
     */
    private void setAvailableBufferValue(int index, int flag)
    {
        // 定位到地址
        long bufferAddress = (index * SCALE) + BASE;
        // 这个标识 / ringbuffer 的大小就是 当前下标对应的序列值
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * 判断指定位置是否可用
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        // 获取序列对应的下标值
        int index = calculateIndex(sequence);
        int flag = calculateAvailabilityFlag(sequence);
        long bufferAddress = (index * SCALE) + BASE;
        // 对比 数值是否是flag
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    /**
     * 获取 这组序列中 可用的序列 返回失效的最小值
     * @param lowerBound
     * @param availableSequence The sequence to scan to.
     * @return
     */
    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            if (!isAvailable(sequence))
            {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    private int calculateAvailabilityFlag(final long sequence)
    {
        return (int) (sequence >>> indexShift);
    }

    /**
     * 根据 掩码来计算在 ringBuffer 的下标
     * @param sequence
     * @return
     */
    private int calculateIndex(final long sequence)
    {
        return ((int) sequence) & indexMask;
    }
}
