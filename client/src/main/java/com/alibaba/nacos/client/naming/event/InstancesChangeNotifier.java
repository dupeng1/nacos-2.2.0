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

package com.alibaba.nacos.client.naming.event;

import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A subscriber to notify eventListener callback.
 *
 * @author horizonzy
 * @since 1.4.1
 */

/**
 * 1、负责接收Nacos服务器的实例变更通知，包括实例的启动、关闭、迁移等操作
 * 当Nacos服务器上的实例发生变化时，该组件会立即将变更通知发送给相关的服务消费者，确保服务的高可用性和稳定性
 * 2、在使用Nacos时，如果你需要将服务实例的变更实时通知给其他服务消费者，那么InstancesChangeNotifier将是
 * 一个非常不错的选择。
 */
public class InstancesChangeNotifier extends Subscriber<InstancesChangeEvent> {
    
    private final String eventScope;
    //key 为 由服务名、组名、集群所构成字符串，可以唯一标识一种服务订阅类型，值为监听该服务的EventListrner的集合
    private final Map<String, ConcurrentHashSet<EventListener>> listenerMap = new ConcurrentHashMap<>();
    
    @JustForTest
    public InstancesChangeNotifier() {
        this.eventScope = UUID.randomUUID().toString();
    }
    
    public InstancesChangeNotifier(String eventScope) {
        this.eventScope = eventScope;
    }
    
    /**
     * register listener.
     *
     * @param groupName   group name
     * @param serviceName serviceName
     * @param clusters    clusters, concat by ','. such as 'xxx,yyy'
     * @param listener    custom listener
     */
    //事件的注册便是将EventListener存储在InstancesChangeNotifier的listenerMap属性当中了
    public void registerListener(String groupName, String serviceName, String clusters, EventListener listener) {
        //将一个EventListener注册到了订阅者的监听者集合 listenerMap
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.computeIfAbsent(key, keyInner -> new ConcurrentHashSet<>());
        eventListeners.add(listener);
    }
    
    /**
     * deregister listener.
     *
     * @param groupName   group name
     * @param serviceName serviceName
     * @param clusters    clusters, concat by ','. such as 'xxx,yyy'
     * @param listener    custom listener
     */
    public void deregisterListener(String groupName, String serviceName, String clusters, EventListener listener) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.get(key);
        if (eventListeners == null) {
            return;
        }
        eventListeners.remove(listener);
        if (CollectionUtils.isEmpty(eventListeners)) {
            listenerMap.remove(key);
        }
    }
    
    /**
     * check serviceName,clusters is subscribed.
     *
     * @param groupName   group name
     * @param serviceName serviceName
     * @param clusters    clusters, concat by ','. such as 'xxx,yyy'
     * @return is serviceName,clusters subscribed
     */
    public boolean isSubscribed(String groupName, String serviceName, String clusters) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.get(key);
        return CollectionUtils.isNotEmpty(eventListeners);
    }
    
    public List<ServiceInfo> getSubscribeServices() {
        List<ServiceInfo> serviceInfos = new ArrayList<>();
        for (String key : listenerMap.keySet()) {
            serviceInfos.add(ServiceInfo.fromKey(key));
        }
        return serviceInfos;
    }
    
    @Override
    public void onEvent(InstancesChangeEvent event) {
        // 1. 获取服务标识
        String key = ServiceInfo
                .getKey(NamingUtils.getGroupedName(event.getServiceName(), event.getGroupName()), event.getClusters());
        // 2. 获得对监听该服务的所有 EventListener
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.get(key);
        if (CollectionUtils.isEmpty(eventListeners)) {
            return;
        }
        // 3. 执行 EventListener 的回调方法
        for (final EventListener listener : eventListeners) {
            // 事件类型转换，这里将InstancesChangeEvent事件转化为了NamingEvent
            // 但这两种事件实现的是不同的类或接口，前者是com.alibaba.nacos.common.notify包下的抽象类，
            // 后者是com.alibaba.nacos.api.naming.listener包下的接口
            final com.alibaba.nacos.api.naming.listener.Event namingEvent = transferToNamingEvent(event);
            // 如果 EventListener 有线程池就异步执行
            if (listener instanceof AbstractEventListener && ((AbstractEventListener) listener).getExecutor() != null) {
                ((AbstractEventListener) listener).getExecutor().execute(() -> listener.onEvent(namingEvent));
            } else {
                // 否则在当前线程中同步执行
                listener.onEvent(namingEvent);
            }
        }
    }
    
    private com.alibaba.nacos.api.naming.listener.Event transferToNamingEvent(
            InstancesChangeEvent instancesChangeEvent) {
        return new NamingEvent(instancesChangeEvent.getServiceName(), instancesChangeEvent.getGroupName(),
                instancesChangeEvent.getClusters(), instancesChangeEvent.getHosts());
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
    }
    
    @Override
    public boolean scopeMatches(InstancesChangeEvent event) {
        return this.eventScope.equals(event.scope());
    }
}
