package com.lmax.disruptor;

/**
 * Experimental poll-based interface for the Disruptor.
 * 拉取事件
 */
public class EventPoller<T>
{
    /**
     * 提供数据对象 实际上就是ringBuffer
     */
    private final DataProvider<T> dataProvider;
    /**
     * 生产者通过 sequencer 向 ringBuffer 申请空间 并写入事件
     */
    private final Sequencer sequencer;
    /**
     * 代表从 哪里开始拉取数据
     */
    private final Sequence sequence;
    /**
     * 代表被阻隔的序列
     */
    private final Sequence gatingSequence;


    public interface Handler<T>
    {
        boolean onEvent(T event, long sequence, boolean endOfBatch) throws Exception;
    }

    /**
     * 拉取事件的3种状态  处理中  空闲  这个 GATING 可能代表 正在被消费者阻塞???
     */
    public enum PollState
    {
        PROCESSING, GATING, IDLE
    }

    public EventPoller(
        final DataProvider<T> dataProvider,     // 就是ringBuffer
        final Sequencer sequencer,              // 一般就是生产者序列对象
        final Sequence sequence,                // 起点
        final Sequence gatingSequence)          // 被阻隔的序列值
    {
        this.dataProvider = dataProvider;
        this.sequencer = sequencer;
        this.sequence = sequence;
        this.gatingSequence = gatingSequence;
    }

    /**
     * 消费者通过该对象来拉取数据???
     * @param eventHandler
     * @return
     * @throws Exception
     */
    public PollState poll(final Handler<T> eventHandler) throws Exception
    {
        final long currentSequence = sequence.get();
        long nextSequence = currentSequence + 1;
        // 获取生产者可用的最下序列 在单生产者下 就是 gatingSequence 的值 该值也代表着 消费者当前这在消费的最小偏移量
        final long availableSequence = sequencer.getHighestPublishedSequence(nextSequence, gatingSequence.get());

        if (nextSequence <= availableSequence)
        {
            boolean processNextEvent;
            long processedSequence = currentSequence;

            try
            {
                do
                {
                    // 从ringbuffer 一个个的取出 生产者创建好的事件对象 (实际上对象一开始就创建了 这里只是对对象进行加工)
                    final T event = dataProvider.get(nextSequence);
                    processNextEvent = eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    processedSequence = nextSequence;
                    nextSequence++;

                }
                while (nextSequence <= availableSequence & processNextEvent);
            }
            finally
            {
                // 消费结束后更新偏移量
                sequence.set(processedSequence);
            }

            return PollState.PROCESSING;
        }
        else if (sequencer.getCursor() >= nextSequence)
        {
            return PollState.GATING;
        }
        else
        {
            return PollState.IDLE;
        }
    }

    public static <T> EventPoller<T> newInstance(
        final DataProvider<T> dataProvider,
        final Sequencer sequencer,
        final Sequence sequence,
        final Sequence cursorSequence,
        final Sequence... gatingSequences)
    {
        Sequence gatingSequence;
        if (gatingSequences.length == 0)
        {
            gatingSequence = cursorSequence;
        }
        else if (gatingSequences.length == 1)
        {
            gatingSequence = gatingSequences[0];
        }
        else
        {
            gatingSequence = new FixedSequenceGroup(gatingSequences);
        }

        return new EventPoller<T>(dataProvider, sequencer, sequence, gatingSequence);
    }

    public Sequence getSequence()
    {
        return sequence;
    }
}
