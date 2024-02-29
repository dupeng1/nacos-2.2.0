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

import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import com.alibaba.nacos.common.utils.ClassUtils;
import com.alibaba.nacos.common.utils.MapUtil;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.nacos.api.exception.NacosException.SERVER_ERROR;

/**
 * Unified Event Notify Center.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */

/**
 * 1、通知中心的功能主要是用【事件】对应的【发布器】来通知对应的【订阅者】来处理事件的，是一个比较典型的观察者模式的实现
 * 2、NotifyCenter拥有所有EnventPulisher的集合
 * 3、某一个具体的EnventPulisher拥有它的所有Subscriber的集合
 * 4、而Subscribe 拥有它的所有EventLsiter的集合
 *
 */
public class NotifyCenter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    
    public static int ringBufferSize;
    
    public static int shareBufferSize;
    
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    
    private static final EventPublisherFactory DEFAULT_PUBLISHER_FACTORY;
    
    private static final NotifyCenter INSTANCE = new NotifyCenter();
    //这个发布器用来处理慢事件
    private DefaultSharePublisher sharePublisher;
    //处理一些普通事件，由EventPublisherFactory来创建
    private static Class<? extends EventPublisher> clazz;
    
    /**
     * Publisher management container.
     * 发布者管理容器
     * key：事件类型的名字
     * value：DEFAULT_PUBLISHER_FACTORY运行的结果，也就是一个DefaultPublisher
     */
    private final Map<String, EventPublisher> publisherMap = new ConcurrentHashMap<>(16);
    
    static {
        // Internal ArrayBlockingQueue buffer size. For applications with high write throughput,
        // this value needs to be increased appropriately. default value is 16384
        // 设置ringBufferSize的大小，如果系统环境变量没有配置则默认16384（默认发布器处理事件的阻塞队列最大长度）
        String ringBufferSizeProperty = "nacos.core.notify.ring-buffer-size";
        ringBufferSize = Integer.getInteger(ringBufferSizeProperty, 16384);
        
        // The size of the public publisher's message staging queue buffer
        String shareBufferSizeProperty = "nacos.core.notify.share-buffer-size";
        // 设置shareBufferSize的大小，如果系统环境变量没有配置则默认1024（共享发布器处理事件的阻塞队列最大长度）
        shareBufferSize = Integer.getInteger(shareBufferSizeProperty, 1024);
        // 加载SPI，【事件发布器】
        final Collection<EventPublisher> publishers = NacosServiceLoader.load(EventPublisher.class);
        Iterator<EventPublisher> iterator = publishers.iterator();
        // 如果没有事件发布器，则使用默认的事假发布器
        if (iterator.hasNext()) {
            clazz = iterator.next().getClass();
        } else {
            clazz = DefaultPublisher.class;
        }
        // 定义lambda表达式，主要用于生产发布器
        DEFAULT_PUBLISHER_FACTORY = (cls, buffer) -> {
            try {
                EventPublisher publisher = clazz.newInstance();
                publisher.init(cls, buffer);
                return publisher;
            } catch (Throwable ex) {
                LOGGER.error("Service class newInstance has error : ", ex);
                throw new NacosRuntimeException(SERVER_ERROR, ex);
            }
        };
        
        try {
            
            // Create and init DefaultSharePublisher instance.
            //创建慢事件共享发布器DefaultSharePublisher
            INSTANCE.sharePublisher = new DefaultSharePublisher();
            //初始化慢事件共享发布器DefaultSharePublisher
            INSTANCE.sharePublisher.init(SlowEvent.class, shareBufferSize);
            
        } catch (Throwable ex) {
            LOGGER.error("Service class newInstance has error : ", ex);
        }
        // 添加线程销毁前的钩子方法
        ThreadUtils.addShutdownHook(NotifyCenter::shutdown);
    }
    
    @JustForTest
    public static Map<String, EventPublisher> getPublisherMap() {
        return INSTANCE.publisherMap;
    }
    
    @JustForTest
    public static EventPublisher getPublisher(Class<? extends Event> topic) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, topic)) {
            return INSTANCE.sharePublisher;
        }
        return INSTANCE.publisherMap.get(topic.getCanonicalName());
    }
    
    @JustForTest
    public static EventPublisher getSharePublisher() {
        return INSTANCE.sharePublisher;
    }
    
    /**
     * Shutdown the several publisher instance which notify center has.
     */
    public static void shutdown() {
        if (!CLOSED.compareAndSet(false, true)) {
            return;
        }
        LOGGER.warn("[NotifyCenter] Start destroying Publisher");
        
        for (Map.Entry<String, EventPublisher> entry : INSTANCE.publisherMap.entrySet()) {
            try {
                EventPublisher eventPublisher = entry.getValue();
                eventPublisher.shutdown();
            } catch (Throwable e) {
                LOGGER.error("[EventPublisher] shutdown has error : ", e);
            }
        }
        
        try {
            INSTANCE.sharePublisher.shutdown();
        } catch (Throwable e) {
            LOGGER.error("[SharePublisher] shutdown has error : ", e);
        }
        
        LOGGER.warn("[NotifyCenter] Destruction of the end");
    }
    
    /**
     * Register a Subscriber. If the Publisher concerned by the Subscriber does not exist, then PublihserMap will
     * preempt a placeholder Publisher with default EventPublisherFactory first.
     *
     * @param consumer subscriber
     */
    //注册订阅者
    public static void registerSubscriber(final Subscriber consumer) {
        registerSubscriber(consumer, DEFAULT_PUBLISHER_FACTORY);
    }
    
    /**
     * Register a Subscriber. If the Publisher concerned by the Subscriber does not exist, then PublihserMap will
     * preempt a placeholder Publisher with specified EventPublisherFactory first.
     *
     * @param consumer subscriber
     * @param factory  publisher factory.
     */
    //注册订阅者
    public static void registerSubscriber(final Subscriber consumer, final EventPublisherFactory factory) {
        // If you want to listen to multiple events, you do it separately,
        // based on subclass's subscribeTypes method return list, it can register to publisher.
        //判断订阅者是不是SmartSubscriber
        if (consumer instanceof SmartSubscriber) {
            for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
                // For case, producer: defaultSharePublisher -> consumer: smartSubscriber.
                //如果是则循环判断事件是否是慢事件
                //如果是慢事件，则直接加入DefaultSharePublisher
                if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                    INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
                }
                //如果是普通事件，则根据普通事件的规范名去DefaultPublisher集合取对应的Publisher。
                else {
                    // For case, producer: defaultPublisher -> consumer: subscriber.
                    addSubscriber(consumer, subscribeType, factory);
                }
            }
            return;
        }
        
        final Class<? extends Event> subscribeType = consumer.subscribeType();
        if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
            INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
            return;
        }
        
        addSubscriber(consumer, subscribeType, factory);
    }
    
    /**
     * Add a subscriber to publisher.
     *
     * @param consumer      subscriber instance.
     * @param subscribeType subscribeType.
     * @param factory       publisher factory.
     */
    //核心逻辑就是将订阅事件、发布者、订阅者三者进行绑定。而发布者与事件通过Map进行维护、发布者与订阅者通过关联关系进行维护
    private static void addSubscriber(final Subscriber consumer, Class<? extends Event> subscribeType,
            EventPublisherFactory factory) {
        // 1. 通过事件类型获得发布者主题
        final String topic = ClassUtils.getCanonicalName(subscribeType);
        synchronized (NotifyCenter.class) {
            // MapUtils.computeIfAbsent is a unsafe method.
            //利用EventPublisherFactory生成一个EventPublisher
            // 以主题为key，以生成的EventPublisher作为值，加入到NotifyCenter实例的publisherMap中
            MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, subscribeType, ringBufferSize);
        }
        // 获取事件对应的Publisher
        EventPublisher publisher = INSTANCE.publisherMap.get(topic);
        //将订阅者加入到发布者中
        if (publisher instanceof ShardedEventPublisher) {
            ((ShardedEventPublisher) publisher).addSubscriber(consumer, subscribeType);
        } else {
            // 添加到subscribers集合
            publisher.addSubscriber(consumer);
        }
    }
    
    /**
     * Deregister subscriber.
     *
     * @param consumer subscriber instance.
     */
    //注销订阅者
    public static void deregisterSubscriber(final Subscriber consumer) {
        if (consumer instanceof SmartSubscriber) {
            for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
                if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                    INSTANCE.sharePublisher.removeSubscriber(consumer, subscribeType);
                } else {
                    removeSubscriber(consumer, subscribeType);
                }
            }
            return;
        }
        
        final Class<? extends Event> subscribeType = consumer.subscribeType();
        if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
            INSTANCE.sharePublisher.removeSubscriber(consumer, subscribeType);
            return;
        }
        
        if (removeSubscriber(consumer, subscribeType)) {
            return;
        }
        throw new NoSuchElementException("The subscriber has no event publisher");
    }
    
    /**
     * Remove subscriber.
     *
     * @param consumer      subscriber instance.
     * @param subscribeType subscribeType.
     * @return whether remove subscriber successfully or not.
     */
    private static boolean removeSubscriber(final Subscriber consumer, Class<? extends Event> subscribeType) {
        
        final String topic = ClassUtils.getCanonicalName(subscribeType);
        EventPublisher eventPublisher = INSTANCE.publisherMap.get(topic);
        if (null == eventPublisher) {
            return false;
        }
        if (eventPublisher instanceof ShardedEventPublisher) {
            ((ShardedEventPublisher) eventPublisher).removeSubscriber(consumer, subscribeType);
        } else {
            eventPublisher.removeSubscriber(consumer);
        }
        return true;
    }
    
    /**
     * Request publisher publish event Publishers load lazily, calling publisher. Start () only when the event is
     * actually published.
     *
     * @param event class Instances of the event.
     */
    //发布事件
    public static boolean publishEvent(final Event event) {
        try {
            return publishEvent(event.getClass(), event);
        } catch (Throwable ex) {
            LOGGER.error("There was an exception to the message publishing : ", ex);
            return false;
        }
    }
    
    /**
     * Request publisher publish event Publishers load lazily, calling publisher.
     *
     * @param eventType class Instances type of the event type.
     * @param event     event instance.
     */
    private static boolean publishEvent(final Class<? extends Event> eventType, final Event event) {
        // 如果是慢事件，则直接调用sharePublisher的publish方法
        if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
            return INSTANCE.sharePublisher.publish(event);
        }
        // 1. 通过事件类型获得发布者主题
        final String topic = ClassUtils.getCanonicalName(eventType);
        // 2. 获得对应主题下的事件发布者
        EventPublisher publisher = INSTANCE.publisherMap.get(topic);
        // 3. 发布事件，事件的发布调用了发布者的publish()方法
        if (publisher != null) {
            return publisher.publish(event);
        }
        if (event.isPluginEvent()) {
            return true;
        }
        LOGGER.warn("There are no [{}] publishers for this event, please register", topic);
        return false;
    }
    
    /**
     * Register to share-publisher.
     *
     * @param eventType class Instances type of the event type.
     * @return share publisher instance.
     */
    public static EventPublisher registerToSharePublisher(final Class<? extends SlowEvent> eventType) {
        return INSTANCE.sharePublisher;
    }
    
    /**
     * Register publisher with default factory.
     *
     * @param eventType    class Instances type of the event type.
     * @param queueMaxSize the publisher's queue max size.
     */
    public static EventPublisher registerToPublisher(final Class<? extends Event> eventType, final int queueMaxSize) {
        return registerToPublisher(eventType, DEFAULT_PUBLISHER_FACTORY, queueMaxSize);
    }
    
    /**
     * Register publisher with specified factory.
     *
     * @param eventType    class Instances type of the event type.
     * @param factory      publisher factory.
     * @param queueMaxSize the publisher's queue max size.
     */
    //NotifyCenter中管理了所有的发布者，每一种事件类型对应一个发布者。
    public static EventPublisher registerToPublisher(final Class<? extends Event> eventType,
            final EventPublisherFactory factory, final int queueMaxSize) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
            return INSTANCE.sharePublisher;
        }
        // 1. 通过事件类型获得发布者主题
        final String topic = ClassUtils.getCanonicalName(eventType);
        synchronized (NotifyCenter.class) {
            // MapUtils.computeIfAbsent is a unsafe method.
            //利用EventPublisherFactory生成一个EventPublisher
            // 以主题为key，以生成的EventPublisher作为值，加入到NotifyCenter实例的publisherMap中
            MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, eventType, queueMaxSize);
        }
        return INSTANCE.publisherMap.get(topic);
    }
    
    /**
     * Register publisher.
     *
     * @param eventType class Instances type of the event type.
     * @param publisher the specified event publisher
     */
    public static void registerToPublisher(final Class<? extends Event> eventType, final EventPublisher publisher) {
        if (null == publisher) {
            return;
        }
        final String topic = ClassUtils.getCanonicalName(eventType);
        synchronized (NotifyCenter.class) {
            INSTANCE.publisherMap.putIfAbsent(topic, publisher);
        }
    }
    
    /**
     * Deregister publisher.
     *
     * @param eventType class Instances type of the event type.
     */
    public static void deregisterPublisher(final Class<? extends Event> eventType) {
        final String topic = ClassUtils.getCanonicalName(eventType);
        EventPublisher publisher = INSTANCE.publisherMap.remove(topic);
        try {
            publisher.shutdown();
        } catch (Throwable ex) {
            LOGGER.error("There was an exception when publisher shutdown : ", ex);
        }
    }
    
}
