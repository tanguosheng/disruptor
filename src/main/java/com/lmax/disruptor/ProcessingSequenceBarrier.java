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
 */
final class ProcessingSequenceBarrier implements SequenceBarrier
{
    private final WaitStrategy waitStrategy;
    private final Sequence dependentSequence;
    private volatile boolean alerted = false;
    private final Sequence cursorSequence;
    private final Sequencer sequencer;

    ProcessingSequenceBarrier(
        final Sequencer sequencer,
        final WaitStrategy waitStrategy,
        final Sequence cursorSequence,
        final Sequence[] dependentSequences)
    {
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;
        if (0 == dependentSequences.length)
        {
            dependentSequence = cursorSequence;
        }
        else
        {
            dependentSequence = new FixedSequenceGroup(dependentSequences);
        }
    }

    @Override
    public long waitFor(final long sequence)
        throws AlertException, InterruptedException, TimeoutException
    {
        checkAlert();

        // WaitStrategy 等待策略，有多种实现:

        // BlockingWaitStrategy:        Disruptor的默认策略是BlockingWaitStrategy。
        //                              在BlockingWaitStrategy内部是使用锁和condition来控制线程的唤醒。
        //                              BlockingWaitStrategy是最低效的策略，
        //                              但其对CPU的消耗最小并且在各种不同部署环境中能提供更加一致的性能表现。

        // SleepingWaitStrategy:        SleepingWaitStrategy 的性能表现跟 BlockingWaitStrategy 差不多，
        //                              对 CPU 的消耗也类似，但其对生产者线程的影响最小，
        //                              通过使用LockSupport.parkNanos(1)来实现循环等待。

        // YieldingWaitStrategy:        YieldingWaitStrategy是可以使用在低延迟系统的策略之一。
        //                              YieldingWaitStrategy将自旋以等待序列增加到适当的值。
        //                              在循环体内，将调用Thread.yield()以允许其他排队的线程运行。
        //                              在要求极高性能且事件处理线数小于 CPU 逻辑核心数的场景中，推荐使用此策略；
        //                              例如，CPU开启超线程的特性。

        // BusySpinWaitStrategy:        性能最好，适合用于低延迟的系统。
        //                              在要求极高性能且事件处理线程数小于CPU逻辑核心数的场景中，
        //                              推荐使用此策略；例如，CPU开启超线程的特性。

        // PhasedBackoffWaitStrategy:   自旋 + yield + 自定义策略，
        //                              CPU资源紧缺，吞吐量和延迟并不重要的场景。

        // waitFor 方法的意义是: 给定一个期望的 sequence, 等待策略返回一个 availableSequence
        // availableSequence <= sequence, 等待超时时 availableSequence < sequence
        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);

        if (availableSequence < sequence)
        {
            // 返回最大可用的 sequence
            return availableSequence;
        }

        // 从 sequence 到 availableSequence，扫描出一个最大的可用的 sequence
        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    @Override
    public long getCursor()
    {
        return dependentSequence.get();
    }

    @Override
    public boolean isAlerted()
    {
        return alerted;
    }

    @Override
    public void alert()
    {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

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