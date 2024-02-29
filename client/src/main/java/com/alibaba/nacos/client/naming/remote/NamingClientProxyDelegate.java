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

package com.alibaba.nacos.client.naming.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.ServerListManager;
import com.alibaba.nacos.client.naming.core.ServiceInfoUpdateService;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientManager;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.constant.Constants.Security.SECURITY_INFO_REFRESH_INTERVAL_MILLS;
import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Delegate of naming client proxy.
 *
 * @author xiweng.yy
 */

/**
 * 1、代理类，对所有NacosNamingService中的方法进行代理，根据实际情况选择http或gRPC协议请求服务端。
 * 2、选择通信方式
 *  gRPC实现（默认）
 *  http实现
 *  3、会根据instance实例是否是临时节点而选择不同的协议。
 *  临时instance：gRPC
 *  持久instance：http
 */
public class NamingClientProxyDelegate implements NamingClientProxy {
    //服务地址列表管理
    private final ServerListManager serverListManager;
    //本地服务更新服务
    private final ServiceInfoUpdateService serviceInfoUpdateService;
    //客户端持有的本地服务信息
    private final ServiceInfoHolder serviceInfoHolder;
    //http请求代理
    private final NamingHttpClientProxy httpClientProxy;
    //grpc请求代理
    private final NamingGrpcClientProxy grpcClientProxy;
    
    private final SecurityProxy securityProxy;
    
    private ScheduledExecutorService executorService;
    
    public NamingClientProxyDelegate(String namespace, ServiceInfoHolder serviceInfoHolder, NacosClientProperties properties,
            InstancesChangeNotifier changeNotifier) throws NacosException {
        //本地服务更新ServiceInfoUpdateService
        this.serviceInfoUpdateService = new ServiceInfoUpdateService(properties, serviceInfoHolder, this,
                changeNotifier);
        //服务地址列表管理
        this.serverListManager = new ServerListManager(properties, namespace);
        //本地服务缓存初始化
        this.serviceInfoHolder = serviceInfoHolder;
        //初始化认证token（1），token主要用于请求认证（如果服务端开启认证）
        this.securityProxy = new SecurityProxy(this.serverListManager.getServerList(),
                NamingHttpClientManager.getInstance().getNacosRestTemplate());
        initSecurityProxy(properties);
        //初始化NamingHttpClientProxy
        this.httpClientProxy = new NamingHttpClientProxy(namespace, securityProxy, serverListManager, properties);
        //初始化NamingGrpcClientProxy
        this.grpcClientProxy = new NamingGrpcClientProxy(namespace, securityProxy, serverListManager, properties,
                serviceInfoHolder);
    }
    
    private void initSecurityProxy(NacosClientProperties properties) {
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r);
            t.setName("com.alibaba.nacos.client.naming.security");
            t.setDaemon(true);
            return t;
        });
        final Properties nacosClientPropertiesView = properties.asProperties();
        this.securityProxy.login(nacosClientPropertiesView);
        this.executorService
                .scheduleWithFixedDelay(() -> securityProxy.login(nacosClientPropertiesView), 0, SECURITY_INFO_REFRESH_INTERVAL_MILLS,
                        TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void registerService(String serviceName, String groupName, Instance instance) throws NacosException {
        //根据实例的ephemeral值来选择客户端使用Client
        //true：Grpc的代理客户端NamingGrpcClientProxy
        //false：Http的代理客户端NamingHttpClientProxy
        getExecuteClientProxy(instance).registerService(serviceName, groupName, instance);
    }
    
    @Override
    public void batchRegisterService(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NAMING_LOGGER.info("batchRegisterInstance instances: {} ,serviceName: {} begin.", instances, serviceName);
        if (CollectionUtils.isEmpty(instances)) {
            NAMING_LOGGER.warn("batchRegisterInstance instances is Empty:{}", instances);
        }
        grpcClientProxy.batchRegisterService(serviceName, groupName, instances);
        NAMING_LOGGER.info("batchRegisterInstance instances: {} ,serviceName: {} finish.", instances, serviceName);
    }
    
    @Override
    public void batchDeregisterService(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NAMING_LOGGER.info("batch DeregisterInstance instances: {} ,serviceName: {} begin.", instances, serviceName);
        if (CollectionUtils.isEmpty(instances)) {
            NAMING_LOGGER.warn("batch DeregisterInstance instances is Empty:{}", instances);
        }
        grpcClientProxy.batchDeregisterService(serviceName, groupName, instances);
        NAMING_LOGGER.info("batch DeregisterInstance instances: {} ,serviceName: {} finish.", instances, serviceName);
    }
    
    @Override
    public void deregisterService(String serviceName, String groupName, Instance instance) throws NacosException {
        getExecuteClientProxy(instance).deregisterService(serviceName, groupName, instance);
    }
    
    @Override
    public void updateInstance(String serviceName, String groupName, Instance instance) throws NacosException {
    
    }
    
    @Override
    public ServiceInfo queryInstancesOfService(String serviceName, String groupName, String clusters, int udpPort,
            boolean healthyOnly) throws NacosException {
        return grpcClientProxy.queryInstancesOfService(serviceName, groupName, clusters, udpPort, healthyOnly);
    }
    
    @Override
    public Service queryService(String serviceName, String groupName) throws NacosException {
        return null;
    }
    
    @Override
    public void createService(Service service, AbstractSelector selector) throws NacosException {
    
    }
    
    @Override
    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        return false;
    }
    
    @Override
    public void updateService(Service service, AbstractSelector selector) throws NacosException {
    
    }
    
    @Override
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException {
        return grpcClientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }
    
    @Override
    public ServiceInfo subscribe(String serviceName, String groupName, String clusters) throws NacosException {
        NAMING_LOGGER.info("[SUBSCRIBE-SERVICE] service:{}, group:{}, clusters:{} ", serviceName, groupName, clusters);
        String serviceNameWithGroup = NamingUtils.getGroupedName(serviceName, groupName);
        String serviceKey = ServiceInfo.getKey(serviceNameWithGroup, clusters);
        // 定时调度UpdateTask。
        // 阅方法先开启定时任务，这个定时任务的主要作用就是用来定时同步服务端的实例信息，并进行本地缓存更新等操作，但是如果是首次这里将会直接返回来走下一步。
        serviceInfoUpdateService.scheduleUpdateIfAbsent(serviceName, groupName, clusters);
        // 获取缓存中的ServiceInfo
        ServiceInfo result = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
        //判断本地缓存是否存在
        if (null == result || !isSubscribed(serviceName, groupName, clusters)) {
            // 如果本地缓存不存在，则进行订阅逻辑处理，基于gRPC协议。直接向服务器发送了一个订阅请求，并返回结果
            result = grpcClientProxy.subscribe(serviceName, groupName, clusters);
        }
        // ServiceInfo本地缓存处理
        serviceInfoHolder.processServiceInfo(result);
        return result;
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, String clusters) throws NacosException {
        NAMING_LOGGER
                .debug("[UNSUBSCRIBE-SERVICE] service:{}, group:{}, cluster:{} ", serviceName, groupName, clusters);
        //从ServiceInfoUpdateService中停止并删除这个服务的定时更新任务
        serviceInfoUpdateService.stopUpdateIfContain(serviceName, groupName, clusters);
        grpcClientProxy.unsubscribe(serviceName, groupName, clusters);
    }
    
    @Override
    public boolean isSubscribed(String serviceName, String groupName, String clusters) throws NacosException {
        return grpcClientProxy.isSubscribed(serviceName, groupName, clusters);
    }
    
    @Override
    public boolean serverHealthy() {
        return grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy();
    }

    /**
     * 根据实例是否瞬时来判断用哪个客户端代理类来进行注册服务
     * 如果当前实例为瞬时对象，则采用gRPC协议NamingGrpcClientProxy）进行请求
     * 否则采用http协议（NamingHttpClientProxy）进行请求
     * 默认为瞬时对象，也就是说，2.0版本中默认采用了gRPC协议进行与Nacos服务进行交互
     * @param instance
     * @return
     */
    private NamingClientProxy getExecuteClientProxy(Instance instance) {
        return instance.isEphemeral() ? grpcClientProxy : httpClientProxy;
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        serviceInfoUpdateService.shutdown();
        serverListManager.shutdown();
        httpClientProxy.shutdown();
        grpcClientProxy.shutdown();
        securityProxy.shutdown();
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
