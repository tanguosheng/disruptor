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
 *
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public final class BatchEventProcessor<T>
    implements EventProcessor
{
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private ExceptionHandler<? super T> exceptionHandler = new FatalExceptionHandler();
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final TimeoutHandler timeoutHandler;
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
        final DataProvider<T> dataProvider,
        final SequenceBarrier sequenceBarrier,
        final EventHandler<? super T> eventHandler)
    {
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;

        if (eventHandler instanceof SequenceReportingEventHandler)
        {
            ((SequenceReportingEventHandler<?>) eventHandler).setSequenceCallback(sequence);
        }

        batchStartAware =
            (eventHandler instanceof BatchStartAware) ? (BatchStartAware) eventHandler : null;
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
        running.set(HALTED);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning()
    {
        return running.get() != IDLE;
    }

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
     *
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
        // 两个线程不能同时执行一个 Processor, Processor 是有状态的
        if (running.compareAndSet(IDLE, RUNNING))
        {
            sequenceBarrier.clearAlert();

            // EventHandler 可以实现 LifecycleAware 接口来获取事件通知
            notifyStart();
            try
            {
                if (running.get() == RUNNING)
                {
                    processEvents();
                }
            }
            finally
            {
                // 发出停机事件
                notifyShutdown();
                // 重置 Processor 的状态为空闲
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

    private void processEvents()
    {
        T event = null;
        // 计算消费者下一个下标
        long nextSequence = sequence.get() + 1L;

        // 消费写在一个死循环中，
        // 用异常(AlertException)来终止消费，而没有用一个if状态位来终止消费
        // 这样每次循环都能少一个if判断，优化执行效率
        while (true)
        {
            try
            {
                // 取得当前能用的 sequence
                final long availableSequence = sequenceBarrier.waitFor(nextSequence);
                if (batchStartAware != null)
                {
                    batchStartAware.onBatchStart(availableSequence - nextSequence + 1);
                }

                // 处理一批事件
                while (nextSequence <= availableSequence)
                {
                    event = dataProvider.get(nextSequence);

                    // 将事件传给消费者
                    // event 当前获取到的事件
                    // nextSequence 当前事件的序号
                    // endOfBatch 是否是批次中最后一个
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }

                // 把消费到的最后一个事件的下标存起来
                sequence.set(availableSequence);
            }
            catch (final TimeoutException e)
            {
                // 执行注册的超时处理器
                notifyTimeout(sequence.get());
            }
            catch (final AlertException ex)
            {
                if (running.get() != RUNNING)
                {
                    break;
                }
            }
            catch (final Throwable ex)
            {
                // 发生异常时执行异常处理器，默认实现为 FatalExceptionHandler, 抛出 RuntimeException
                exceptionHandler.handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }

    private void earlyExit()
    {
        notifyStart();
        notifyShutdown();
    }

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