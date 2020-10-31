package com.lmax.disruptor.myCase;

import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

/**
 * 生产者
 */
public class LongEventProducer {
    public final RingBuffer<LongEvent> ringBuffer;
    public final String producerName;

    public LongEventProducer(RingBuffer<LongEvent> ringBuffer, String producerName) {
        this.ringBuffer = ringBuffer;
        this.producerName = producerName;
    }

    public void onData(ByteBuffer byteBuffer) {
        // 1.ringBuffer 事件队列 下一个槽
        long sequence = ringBuffer.next();
        Long data = null;
        try {
            // 2.取出空的事件队列
            LongEvent longEvent = ringBuffer.get(sequence);
            data = byteBuffer.getLong(0);
            // 3.获取事件队列传递的数据
            longEvent.setValue(data);
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } finally {
            System.out.println(producerName + " 准备发送数据：" + data);
            // 4.发布事件
            ringBuffer.publish(sequence);
        }
    }
}
