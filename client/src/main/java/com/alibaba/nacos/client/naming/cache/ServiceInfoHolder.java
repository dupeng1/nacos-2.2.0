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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming client service information holder.
 *
 * @author xiweng.yy
 */

/**
 * 客户端持有的本地服务信息，每次客户端从注册中心获取新的服务信息时都会调用该类
 * 核心作用：
 * 1、缓存ServiceInfo
 * 2、判断ServiceInfo是否更新
 * 3、写入本地缓存
 * 4、发布变更事件
 */
public class ServiceInfoHolder implements Closeable {
    
    private static final String JM_SNAPSHOT_PATH_PROPERTY = "JM.SNAPSHOT.PATH";
    
    private static final String FILE_PATH_NACOS = "nacos";
    
    private static final String FILE_PATH_NAMING = "naming";
    
    private static final String USER_HOME_PROPERTY = "user.home";
    //ServiceInfo缓存，Nacos客户端对服务端获取到的注册信息的第一层缓存
    //key->服务名（格式：groupName@@serviceName@@clusters）
    private final ConcurrentMap<String, ServiceInfo> serviceInfoMap;
    //故障转移反应者
    private final FailoverReactor failoverReactor;
    
    private final boolean pushEmptyProtection;
    //地缓存目录cacheDir，用于指定本地缓存的根目录和故障转移的根目录。
    private String cacheDir;
    
    private String notifierEventScope;
    
    public ServiceInfoHolder(String namespace, String notifierEventScope, NacosClientProperties properties) {
        // 初始化并且生成缓存目录
        initCacheDir(namespace, properties);
        //根据properties是否设置namingLoadCacheAtStart，如果设置为true（默认false），
        // 则调用DiskCache从本地缓存目录读取服务列表加载到serviceInfoMap集合
        if (isLoadCacheAtStart(properties)) {
            //根据配置从本地缓存初始化服务，默认false
            this.serviceInfoMap = new ConcurrentHashMap<>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<>(16);
        }
        // 初始化故障转移反应者，this为ServiceHolder当前对象，这里可以立即为两者相互持有对方的引用
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        //从properties配置中读取namingPushEmptyProtection配置，如果namingPushEmptyProtection=true（默认false），
        // 表示在服务没有有效（健康）实例时，是否开启保护，开启后则会使用旧的服务实例；
        this.pushEmptyProtection = isPushEmptyProtect(properties);
        this.notifierEventScope = notifierEventScope;
    }

    //生成缓存目录的操作，默认路径：${user.home}/nacos/naming/public，也可以通过System.setProperty(“JM.SNAPSHOT.PATH”)自定义
    //本地服务列表缓存在磁盘的文件目录是：{user.home}/nacos/naming/public/
    //故障转移信息也存储在该目录下
    private void initCacheDir(String namespace, NacosClientProperties properties) {
        String jmSnapshotPath = properties.getProperty(JM_SNAPSHOT_PATH_PROPERTY);
    
        String namingCacheRegistryDir = "";
        if (properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR) != null) {
            namingCacheRegistryDir = File.separator + properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR);
        }
        
        if (!StringUtils.isBlank(jmSnapshotPath)) {
            cacheDir = jmSnapshotPath + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        } else {
            cacheDir = properties.getProperty(USER_HOME_PROPERTY) + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        }
    }
    
    private boolean isLoadCacheAtStart(NacosClientProperties properties) {
        boolean loadCacheAtStart = false;
        if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
            loadCacheAtStart = ConvertUtils
                    .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
        }
        return loadCacheAtStart;
    }
    
    private boolean isPushEmptyProtect(NacosClientProperties properties) {
        boolean pushEmptyProtection = false;
        if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION))) {
            pushEmptyProtection = ConvertUtils
                    .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION));
        }
        return pushEmptyProtection;
    }
    
    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    //根据服务名、服务分组、集群获取服务ServiceInfo
    public ServiceInfo getServiceInfo(final String serviceName, final String groupName, final String clusters) {
        NAMING_LOGGER.debug("failover-mode: {}", failoverReactor.isFailoverSwitch());
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        String key = ServiceInfo.getKey(groupedServiceName, clusters);
        //如果开启了故障转移，那么从故障转移缓存文件中读取服务；
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }
        //默认不会开启故障转移，从serviceInfoMap中取
        return serviceInfoMap.get(key);
    }
    
    /**
     * Process service json.
     *
     * @param json service json
     * @return service info
     */
    //服务信息处理
    public ServiceInfo processServiceInfo(String json) {
        ServiceInfo serviceInfo = JacksonUtils.toObj(json, ServiceInfo.class);
        serviceInfo.setJsonFromServer(json);
        return processServiceInfo(serviceInfo);
    }
    
    /**
     * Process service info.
     *
     * @param serviceInfo new service info
     * @return service info
     */
    //服务信息处理，包括更新缓存服务、发布事件、更新本地文件
    public ServiceInfo processServiceInfo(ServiceInfo serviceInfo) {
        String serviceKey = serviceInfo.getKey();
        //判断ServiceInfo的key是否为null
        if (serviceKey == null) {
            return null;
        }
        //判断ServiceInfo信息的完整性
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());
        //如果服务没有实例或者服务没有健康实例，在开启pushEmptyProtection时，直接返回旧的服务
        if (isEmptyOrErrorPush(serviceInfo)) {
            //empty or error push, just ignore
            return oldService;
        }
        // 缓存服务信息
        serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
        // 判断注册的实例信息是否已变更
        boolean changed = isChangedServiceInfo(oldService, serviceInfo);
        if (StringUtils.isBlank(serviceInfo.getJsonFromServer())) {
            serviceInfo.setJsonFromServer(JacksonUtils.toJson(serviceInfo));
        }
        // 监控服务监控缓存Map的大小
        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());
        // 服务实例以更变
        if (changed) {
            NAMING_LOGGER.info("current ips:({}) service: {} -> {}", serviceInfo.ipCount(), serviceInfo.getKey(),
                    JacksonUtils.toJson(serviceInfo.getHosts()));
            // 添加实例变更事件，会被订阅者执行
            /**
             * NotifyCenter中进行事件发布，发布的核心逻辑是：
             * 1. 根据InstancesChangeEvent事件类型，获得对应的CanonicalName
             * 2. 将CanonicalName作为key，从NotifyCenter.publisherMap中获取对应的事件发布者(EventPublisher)
             * 3. EventPublisher将InstancesChangeEvent事件进行发布
             */
            NotifyCenter.publishEvent(new InstancesChangeEvent(notifierEventScope, serviceInfo.getName(), serviceInfo.getGroupName(),
                    serviceInfo.getClusters(), serviceInfo.getHosts()));
            // 记录Service本地文件
            DiskCache.write(serviceInfo, cacheDir);
        }
        return serviceInfo;
    }
    
    private boolean isEmptyOrErrorPush(ServiceInfo serviceInfo) {
        return null == serviceInfo.getHosts() || (pushEmptyProtection && !serviceInfo.validate());
    }
    
    private boolean isChangedServiceInfo(ServiceInfo oldService, ServiceInfo newService) {
        if (null == oldService) {
            NAMING_LOGGER.info("init new ips({}) service: {} -> {}", newService.ipCount(), newService.getKey(),
                    JacksonUtils.toJson(newService.getHosts()));
            return true;
        }
        if (oldService.getLastRefTime() > newService.getLastRefTime()) {
            NAMING_LOGGER.warn("out of date data received, old-t: {}, new-t: {}", oldService.getLastRefTime(),
                    newService.getLastRefTime());
            return false;
        }
        boolean changed = false;
        Map<String, Instance> oldHostMap = new HashMap<>(oldService.getHosts().size());
        for (Instance host : oldService.getHosts()) {
            oldHostMap.put(host.toInetAddr(), host);
        }
        Map<String, Instance> newHostMap = new HashMap<>(newService.getHosts().size());
        for (Instance host : newService.getHosts()) {
            newHostMap.put(host.toInetAddr(), host);
        }
        
        Set<Instance> modHosts = new HashSet<>();
        Set<Instance> newHosts = new HashSet<>();
        Set<Instance> remvHosts = new HashSet<>();
        
        List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<>(
                newHostMap.entrySet());
        for (Map.Entry<String, Instance> entry : newServiceHosts) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(), oldHostMap.get(key).toString())) {
                modHosts.add(host);
                continue;
            }
            
            if (!oldHostMap.containsKey(key)) {
                newHosts.add(host);
            }
        }
        
        for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (newHostMap.containsKey(key)) {
                continue;
            }

            //add to remove hosts
            remvHosts.add(host);
        }
        
        if (newHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("new ips({}) service: {} -> {}", newHosts.size(), newService.getKey(),
                    JacksonUtils.toJson(newHosts));
        }
        
        if (remvHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("removed ips({}) service: {} -> {}", remvHosts.size(), newService.getKey(),
                    JacksonUtils.toJson(remvHosts));
        }
        
        if (modHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("modified ips({}) service: {} -> {}", modHosts.size(), newService.getKey(),
                    JacksonUtils.toJson(modHosts));
        }
        return changed;
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        failoverReactor.shutdown();
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
