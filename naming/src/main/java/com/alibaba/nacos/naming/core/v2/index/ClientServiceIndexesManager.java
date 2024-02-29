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

package com.alibaba.nacos.naming.core.v2.index;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client and service index manager.
 *
 * @author xiweng.yy
 */

/**
 * 1、在Nacos服务端，客户端服务注册、服务订阅相关的信息存放在类ClientServiceIndexesManager里面的
 * 客户端服务实例注册、客户端服务订阅之后，ClientServiceIndexesManager中有两个集合:publisherIndexes、subscriberIndexes
 * 2、作为一个订阅者，订阅多个事件
 *  ClientOperationEvent.ClientRegisterServiceEvent：客户端服务注册
 *  ClientOperationEvent.ClientDeregisterServiceEvent：客户端服务反注册
 *  ClientOperationEvent.ClientSubscribeServiceEvent：客户端服务订阅
 *  ClientOperationEvent.ClientUnsubscribeServiceEvent：客户端服务反订阅
 *  ClientEvent.ClientDisconnectEvent：客户端失联
 *  这样在有客户端进行注册、订阅或者连接断开之后就对集合中数据进行相应的维护
 */
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {
    //Service与发布clientId
    //用于存放注册的服务及服务对应的服务客户端（一般就是clientId）
    private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();
    //Service与订阅clientId
    //用于存放服务及对应订阅该服务的客户端信息（一般就是clientId）
    private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();
    
    public ClientServiceIndexesManager() {
        //在NotifyCenter中注册了自己，Nacos中订阅者一般都是在创建的时候，也就是构造方法中注册
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }
    
    public Collection<String> getAllClientsRegisteredService(Service service) {
        return publisherIndexes.containsKey(service) ? publisherIndexes.get(service) : new ConcurrentHashSet<>();
    }
    
    public Collection<String> getAllClientsSubscribeService(Service service) {
        return subscriberIndexes.containsKey(service) ? subscriberIndexes.get(service) : new ConcurrentHashSet<>();
    }
    
    public Collection<Service> getSubscribedService() {
        return subscriberIndexes.keySet();
    }
    
    /**
     * Clear the service index without instances.
     *
     * @param service The service of the Nacos.
     */
    public void removePublisherIndexesByEmptyService(Service service) {
        if (publisherIndexes.containsKey(service) && publisherIndexes.get(service).isEmpty()) {
            publisherIndexes.remove(service);
        }
    }
    
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientOperationEvent.ClientRegisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientDeregisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientSubscribeServiceEvent.class);
        result.add(ClientOperationEvent.ClientUnsubscribeServiceEvent.class);
        result.add(ClientEvent.ClientDisconnectEvent.class);
        return result;
    }
    
    @Override
    public void onEvent(Event event) {
        if (event instanceof ClientEvent.ClientDisconnectEvent) {
            handleClientDisconnect((ClientEvent.ClientDisconnectEvent) event);
        } else if (event instanceof ClientOperationEvent) {
            handleClientOperation((ClientOperationEvent) event);
        }
    }
    
    private void handleClientDisconnect(ClientEvent.ClientDisconnectEvent event) {
        //接收到客户端失去连接事件后，找到该客户端的所有订阅服务，从订阅服务的客户端集合中删除该客户端；
        Client client = event.getClient();
        for (Service each : client.getAllSubscribeService()) {
            removeSubscriberIndexes(each, client.getClientId());
        }
        DeregisterInstanceReason reason = event.isNative()
                ? DeregisterInstanceReason.NATIVE_DISCONNECTED : DeregisterInstanceReason.SYNCED_DISCONNECTED;
        long currentTimeMillis = System.currentTimeMillis();
        for (Service each : client.getAllPublishedService()) {
            //接收到客户端失去连接事件后，找到该客户端的的注册服务，
            // 向服务的客户端集合publisherIndexes删除该注册客户端
            removePublisherIndexes(each, client.getClientId());
            InstancePublishInfo instance = client.getInstancePublishInfo(each);
            //然后发布事件ServiceEvent.ServiceChangedEvent
            NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(currentTimeMillis,
                    "", false, reason, each.getNamespace(), each.getGroup(), each.getName(),
                    instance.getIp(), instance.getPort()));
        }
    }
    
    private void handleClientOperation(ClientOperationEvent event) {
        Service service = event.getService();
        String clientId = event.getClientId();
        if (event instanceof ClientOperationEvent.ClientRegisterServiceEvent) {
            // 处理ClientRegisterServiceEvent事件
            addPublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientDeregisterServiceEvent) {
            // 处理ClientDeregisterServiceEvent事件
            removePublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientSubscribeServiceEvent) {
            // 处理ClientSubscribeServiceEvent事件
            addSubscriberIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientUnsubscribeServiceEvent) {
            // 处理ClientUnsubscribeServiceEvent事件
            removeSubscriberIndexes(service, clientId);
        }
    }
    //建立Service与发布Client的关系
    private void addPublisherIndexes(Service service, String clientId) {
        //接收到客户端注册后，向服务对应的客户端集合publisherIndexes添加该注册客户端，
        publisherIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        publisherIndexes.get(service).add(clientId);
        // 既然该服务有新的客户端注册，说明服务发生了变更，所以还会发布服务变更事件ServiceEvent.ServiceChangedEvent
        // 又发布了一个服务变化事件，代表服务注册表变更，由ClientServiceIndexesManager监听处理
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
    }
    
    private void removePublisherIndexes(Service service, String clientId) {
        //接收到客户端反注册后，向服务对应的客户端集合publisherIndexes删除该注册客户端，
        publisherIndexes.computeIfPresent(service, (s, ids) -> {
            ids.remove(clientId);
            // 既然该服务有客户端移除注册，说明服务发生了变更，所以还会发布服务变更事件ServiceEvent.ServiceChangedEvent
            NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
            return ids.isEmpty() ? null : ids;
        });
    }
    
    private void addSubscriberIndexes(Service service, String clientId) {
        //接收到客户端订阅服务后，向订阅该服务的客户端集合subscriberIndexes添加该服务的订阅客户端
        subscriberIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        // Fix #5404, Only first time add need notify event.
        if (subscriberIndexes.get(service).add(clientId)) {
            // 如果首次添加（即该客户端首次订阅）就发布服务订阅变更事件ServiceEvent.ServiceSubscribedEvent；
            NotifyCenter.publishEvent(new ServiceEvent.ServiceSubscribedEvent(service, clientId));
        }
    }
    
    private void removeSubscriberIndexes(Service service, String clientId) {
        //接收到客户端反订阅服务后，向订阅该服务的客户端集合subscriberIndexes删除该服务的订阅客户端；
        if (!subscriberIndexes.containsKey(service)) {
            return;
        }
        subscriberIndexes.get(service).remove(clientId);
        if (subscriberIndexes.get(service).isEmpty()) {
            subscriberIndexes.remove(service);
        }
    }
}
