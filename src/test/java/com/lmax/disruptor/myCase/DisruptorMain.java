package com.lmax.disruptor.myCase;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DisruptorMain {

    public static void main(String[] args) throws InterruptedException {

        // 1.创建一个可缓存的线程 提供线程来触发 Consumer 的事件处理
        ExecutorService executor = Executors.newCachedThreadPool();

        // 2.创建工厂
        EventFactory<LongEvent> eventFactory = new LongEventFactory();

        // 3.创建 ringBuffer 大小, ringBufferSize 大小一定要是2的N次方
        int ringBufferSize = 8;

        // 4.创建 Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(
                eventFactory,
                ringBufferSize,
                executor,
                ProducerType.MULTI, // 多重生产者模式
                new YieldingWaitStrategy());

        // 5.连接消费端方法
        disruptor.handleEventsWith(new LongEventHandler("消费者1"),
                                   new LongEventHandler("消费者2"),
                                   new LongEventHandler("消费者3"));

        // 6.启动
        disruptor.start();

        // 7.创建 RingBuffer 容器
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        Thread producerThread1 = new Thread(new ProducerRunnable(
                ringBuffer, "生产者1", 1, 100));
        Thread producerThread2 = new Thread(new ProducerRunnable(
                ringBuffer, "生产者2", 101, 200));
        Thread producerThread3 = new Thread(new ProducerRunnable(
                ringBuffer, "生产者3", 201, 300));

        // 开启多个生产者线程
        producerThread1.start();
        producerThread2.start();
        producerThread3.start();

        // 主线程等待生产者生产完毕
        producerThread1.join();
        producerThread2.join();

        //10.关闭 disruptor 和 executor
        disruptor.shutdown();
        executor.shutdown();
    }

    static class ProducerRunnable implements Runnable {

        // 8.创建生产者
        private final LongEventProducer producer;

        private final int begin;

        private final int end;

        // 9.指定缓冲区大小
        private final ByteBuffer byteBuffer = ByteBuffer.allocate(8);

        public ProducerRunnable(
                RingBuffer<LongEvent> ringBuffer, String producerName, int begin, int end) {
            this.begin = begin;
            this.end = end;
            // 初始化生产者
            this.producer = new LongEventProducer(ringBuffer, producerName);
        }

        @Override
        public void run() {
            for (int i = begin; i <= end; i++) {
                byteBuffer.putLong(0, i);
                producer.onData(byteBuffer);
            }
        }
    }

}
