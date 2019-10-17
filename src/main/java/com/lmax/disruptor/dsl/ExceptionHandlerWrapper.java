package com.lmax.disruptor.dsl;

import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.FatalExceptionHandler;

/**
 * 异常处理器包装类
 * @param <T>
 */
public class ExceptionHandlerWrapper<T> implements ExceptionHandler<T> {

    /**
     * 该对象处理异常就是打印日志 作为默认实现  外部通过调用 switchTo 更换真正的异常处理器
     */
    private ExceptionHandler<? super T> delegate = new FatalExceptionHandler();

    public void switchTo(final ExceptionHandler<? super T> exceptionHandler)
    {
        this.delegate = exceptionHandler;
    }

    @Override
    public void handleEventException(final Throwable ex, final long sequence, final T event)
    {
        delegate.handleEventException(ex, sequence, event);
    }

    @Override
    public void handleOnStartException(final Throwable ex)
    {
        delegate.handleOnStartException(ex);
    }

    @Override
    public void handleOnShutdownException(final Throwable ex)
    {
        delegate.handleOnShutdownException(ex);
    }
}
