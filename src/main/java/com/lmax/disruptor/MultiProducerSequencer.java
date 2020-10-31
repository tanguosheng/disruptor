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
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
    private static final Unsafe UNSAFE = Util.getUnsafe();

    // 当前数组第一个元素地址相对于数组起始地址的偏移值
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);
    // 数组每个坑位在内存中占位长度大小
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    private final int[] availableBuffer;
    // 掩码
    private final int indexMask;
    // n
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];

        // bufferSize 为2的n次幂，那么掩码为2的n次幂-1，它的二进制为 n-1 个 1
        indexMask = bufferSize - 1;
        // 用 bufferSize 计算出 n 的值
        indexShift = Util.log2(bufferSize);
        // 初始化 buffer
        initialiseAvailableBuffer();
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
    {
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = gatingSequenceCache.get();

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n)
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        // 整个位置申请过程在一个大循环里，服务于CAS
        do
        {
            // 当前下标位置
            current = cursor.get();

            // 右移n个后的下标位置
            next = current + n;

            // 计算右移动 n 个后相对于 buffer 溢出多少的下标位置，
            // 下边用这个值和最慢消费者消费下标做比对，
            // 如果 wrapPoint 比 最慢消费者的下标大说明扣圈了，不能写入
            long wrapPoint = next - bufferSize;

            // gatingSequenceCache 缓存着最小的消费者消费到的下标位置
            long cachedGatingSequence = gatingSequenceCache.get();

            // 1. wrapPoint 和 cachedGatingSequence 比较，判断是否有扣圈风险
            // 2. 正常情况下最慢消费者的下标不会比当前下标大，但是在多线程并发执行这段代码时就不一定了：
            //    取 cachedGatingSequence 值在取 current 值之后，有其他线程先于当前线程进入下面的if，
            //    也就是说其他线程发现扣圈风险先进if，而此时消费者在并发消费，
            //    计算过的最小消费下标可能就比当前拿到的旧 current 下标大，说明当前线程拿到的 current 已经过期
            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
            {
                // 取得最小的 gatingSequences, 也就是消费最慢的下标位置，
                // 这个值比 gatingSequenceCache 存的值更新，gatingSequence >= gatingSequenceCache
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);

                // 如果比整组最小的 gatingSequence 大，此时肯定扣圈了，如果写入的话会覆盖掉还未消费的数据
                if (wrapPoint > gatingSequence)
                {
                    // 稍等 1 纳秒，等待消费者消费后再重新判断整个流程
                    LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?

                    // 这里并没有更新 gatingSequenceCache 的值，
                    continue;
                }

                // 走到这里说明没扣圈，缓存一下新的最慢消费者下标位置
                gatingSequenceCache.set(gatingSequence);

                // 重新循环
            }
            // 肯定没有扣圈时候试着用cas改写全局光标位置，
            // 如果修改失败说明有其他线程先修改了cursor，发生并发，重新来过
            else if (cursor.compareAndSet(current, next))
            {
                // 修改成功跳出循环，说明申请到了位置，可以向队列中写数据了
                break;
            }
        }
        while (true);

        return next;
    }

    /**
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
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        long produced = cursor.get();
        return getBufferSize() - (produced - consumed);
    }

    private void initialiseAvailableBuffer()
    {
        // PS：emmmmmm..... 不知道为什么要这么写, 直接 i >= 0 就好了，"!=" 要比 ">=" 会更快一些吗？？？

        for (int i = availableBuffer.length - 1; i != 0; i--)
        {
            // 初始化 Buffer，所有坑位初值 = -1
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        setAvailable(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
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
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    private void setAvailable(final long sequence)
    {
        // 将 sequence 所代表的实际坑位赋值为 sequence所在第x圈来表示坑位可用
        // 比如 sequence = 8 (第9个), bufferSize = 4,
        // 那么 sequence 在 buffer中实际位置 = 0(第一个), sequence 所在圈数 = 2（圈数从0开始数, 第3圈）
        // 那么 buffer[0] = 2 来表示 sequence 所在的坑位可用
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    private void setAvailableBufferValue(int index, int flag)
    {
        // buffer 中每个坑位的地址值
        // SCALE = 每个坑位所占大小
        // BASE = buffer 第一个元素在当前数组的起始地址
        long bufferAddress = (index * SCALE) + BASE;

        // 把 flag 放到数组 index 位置
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        // 计算sequence在数组中实际下标
        int index = calculateIndex(sequence);
        // flag 为 sequence 在 buffer 的 第几圈, flag 从0开始计数
        int flag = calculateAvailabilityFlag(sequence);
        // 计算 sequence 在数组上的地址
        long bufferAddress = (index * SCALE) + BASE;
        // 获取对象中 bufferAddress 偏移地址对应的整型field的值, 支持volatile load语义。
        // 相当于 availableBuffer[bufferAddress]
        // 为什么 availableBuffer[bufferAddress] == sequence 在 buffer 的 第几圈 来判断是否可用？see: setAvailable()
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            // 如果 sequence 位置还不可用
            if (!isAvailable(sequence))
            {
                // 返回最后一个可用位置 sequence
                return sequence - 1;
            }
        }
        return availableSequence;
    }

    private int calculateAvailabilityFlag(final long sequence)
    {
        // sequence 无符号右移 n 位 得到 sequence 在 buffer 的第几圈
        return (int) (sequence >>> indexShift);
    }

    private int calculateIndex(final long sequence)
    {
        // indexMask 为 n-1 个 1，和这个数做 & 运算时得到当前序号在数组中的实际位置
        return ((int) sequence) & indexMask;
    }
}
