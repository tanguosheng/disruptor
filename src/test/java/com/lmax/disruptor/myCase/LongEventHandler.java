package com.lmax.disruptor.myCase;

import com.lmax.disruptor.EventHandler;

/**
 * 消费者
 */
public class LongEventHandler implements EventHandler<LongEvent> {

    private final String consumerName;

    public LongEventHandler(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * @param event      published to the {@link RingBuffer} 当前消费的事件
     * @param sequence   of the event being processed 当前消费事件的下标序号
     * @param endOfBatch flag to indicate if this is the last event in a batch from the {@link RingBuffer} 是否是批次中最后一个任务
     * @throws Exception
     */
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {
        System.out.println(consumerName + " 消费:" + event.getValue());
        Thread.sleep(10);
    }

}
