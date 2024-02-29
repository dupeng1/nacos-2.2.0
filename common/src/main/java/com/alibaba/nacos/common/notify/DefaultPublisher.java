/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * The default event publisher implementation.
 *
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */

/**
 * 实现了EventPublisher接口和Thread类
 * 也就是说EventPublisher可以作为一个独立的线程运行
 * 线程会在发布者初始化的时候被启动
 */
public class DefaultPublisher extends Thread implements EventPublisher {
    
    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    
    private volatile boolean initialized = false;
    
    private volatile boolean shutdown = false;
    //事件类型
    private Class<? extends Event> eventType;
    //订阅者
    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<>();
    
    private int queueMaxSize = -1;
    //队列
    private BlockingQueue<Event> queue;
    
    protected volatile Long lastEventSequence = -1L;
    
    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");
    
    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        // 设置为守护线程
        setDaemon(true);
        // 设置线程名
        setName("nacos.publisher-" + type.getName());
        this.eventType = type;
        // 设置事件队列最大值
        this.queueMaxSize = bufferSize;
        // 初始化事件队列
        this.queue = new ArrayBlockingQueue<>(bufferSize);
        // 启动线程
        start();
    }
    
    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }
    
    @Override
    public synchronized void start() {
        // 如果没有初始化，则进行初始化
        if (!initialized) {
            // start just called once
            // start方法只会执行一次
            super.start();
            // 如果queueMaxSize == -1，则使用ringBufferSize
            if (queueMaxSize == -1) {
                queueMaxSize = ringBufferSize;
            }
            // 初始化完成
            initialized = true;
        }
    }
    
    @Override
    public long currentEventSize() {
        return queue.size();
    }
    
    @Override
    public void run() {
        // 这里才是主逻辑
        openEventHandler();
    }
    
    void openEventHandler() {
        try {
            // 等待60s，有订阅者后直接执行下一步
            // This variable is defined to resolve the problem which message overstock in the queue.
            int waitTimes = 60;
            // To ensure that messages are not lost, enable EventHandler when
            // waiting for the first Subscriber to register
            // 死循环延迟，线程启动最大延时60秒，这个主要是为了解决消息积压的问题。
            while (!shutdown && !hasSubscriber() && waitTimes > 0) {
                ThreadUtils.sleep(1000L);
                waitTimes--;
            }
            // 死循环不断的从队列中取出Event，并通知订阅者Subscriber执行Event
            while (!shutdown) {
                // 从队列中取出Event，阻塞在queue的take方法上，当拿到一个event后就会执行receiveEvent方法，然后循环下一个事件的到来
                final Event event = queue.take();
                receiveEvent(event);
                UPDATER.compareAndSet(this, lastEventSequence, Math.max(lastEventSequence, event.sequence()));
            }
        } catch (Throwable ex) {
            LOGGER.error("Event listener exception : ", ex);
        }
    }
    
    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }
    
    @Override
    public boolean publish(Event event) {
        checkIsStart();
        // 向队列中插入事件元素
        // 插入事件到队列，queue是在DefaultPublisher中定义的BlockingQueue，队列已满则插入失败
        boolean success = this.queue.offer(event);
        // 判断是否成功插入
        if (!success) {
            // 如果插入失败，则手动执行，同步发送事件（就是直接处理该事件）
            LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            receiveEvent(event);
            return true;
        }
        return true;
    }
    
    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Receive and notifySubscriber to process the event.
     *
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        final long currentEventSequence = event.sequence();
        
        if (!hasSubscriber()) {
            LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
            return;
        }
        
        // Notification single event listener
        //遍历该发布者的所有订阅者
        for (Subscriber subscriber : subscribers) {
            if (!subscriber.scopeMatches(event)) {
                continue;
            }
            
            // Whether to ignore expiration events
            // 判断事件是否已经过期
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                LOGGER.debug("[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                        event.getClass());
                continue;
            }
            
            // Because unifying smartSubscriber and subscriber, so here need to think of compatibility.
            // Remove original judge part of codes.
            //通知订阅者
            notifySubscriber(subscriber, event);
        }
    }
    
    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {
        
        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        //当Subscriber拥有自己的线程池时，会在Subscriber的线程池中执行异步回调，否则就在DefaultPublisher自身的线程中执行回调
        //一个事件就是一个线程
        final Runnable job = () -> subscriber.onEvent(event);
        final Executor executor = subscriber.executor();
        
        if (executor != null) {
            executor.execute(job);
        } else {
            try {
                job.run();
            } catch (Throwable e) {
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
