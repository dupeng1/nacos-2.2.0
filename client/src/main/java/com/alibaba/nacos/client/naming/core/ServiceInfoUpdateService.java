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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Service information update service.
 *
 * @author xiweng.yy
 */

/**
 * 本地服务更新服务，需要更新的服务信息就是当前客户端订阅的服务
 */
public class ServiceInfoUpdateService implements Closeable {
    
    private static final long DEFAULT_DELAY = 1000L;
    
    private static final int DEFAULT_UPDATE_CACHE_TIME_MULTIPLE = 6;

    // 获取服务信息结果缓存
    // key：groupName@@serviceName@@clusters，服务信息key
    // value：ScheduledFuture，执行的线程是UpdateTask
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<>();

    //客户端持有的服务信息
    private final ServiceInfoHolder serviceInfoHolder;

    //获取服务信息线程池
    private final ScheduledExecutorService executor;
    
    private final NamingClientProxy namingClientProxy;

    //负责订阅事件InstancesChangeEvent，用来判断服务是否订阅，如果没有订阅，获取服务信息任务不需要执行
    private final InstancesChangeNotifier changeNotifier;
    
    private final boolean asyncQuerySubscribeService;
    
    public ServiceInfoUpdateService(NacosClientProperties properties, ServiceInfoHolder serviceInfoHolder,
            NamingClientProxy namingClientProxy, InstancesChangeNotifier changeNotifier) {
        this.asyncQuerySubscribeService = isAsyncQueryForSubscribeService(properties);
        //初始化一个线程池，线程池相关信息如下：
        //线程名：com.alibaba.nacos.client.naming.updater
        //核心线程数：从properties中获取配置namingPollingThreadCount的值，如果没有设置，默认值根据服务器CPU数来决定，（2的N次幂最接近不超过或者等于1倍CPU数的那个数）/2，最小值1；
        //定时任务执行的线程是UpdateTask
        this.executor = new ScheduledThreadPoolExecutor(initPollingThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.naming.updater"));
        this.serviceInfoHolder = serviceInfoHolder;
        this.namingClientProxy = namingClientProxy;
        this.changeNotifier = changeNotifier;
    }
    
    private boolean isAsyncQueryForSubscribeService(NacosClientProperties properties) {
        if (properties == null || !properties.containsKey(PropertyKeyConst.NAMING_ASYNC_QUERY_SUBSCRIBE_SERVICE)) {
            return false;
        }
        return ConvertUtils.toBoolean(properties.getProperty(PropertyKeyConst.NAMING_ASYNC_QUERY_SUBSCRIBE_SERVICE), false);
    }
    
    private int initPollingThreadCount(NacosClientProperties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }
        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
                UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }
    
    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    //为不同的服务创建不同的ScheduledFuture任务
    public void scheduleUpdateIfAbsent(String serviceName, String groupName, String clusters) {
        if (!asyncQuerySubscribeService) {
            return;
        }
        //groupName@@serviceName@@clusters
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (futureMap.get(serviceKey) != null) {
            return;
        }
        synchronized (futureMap) {
            if (futureMap.get(serviceKey) != null) {
                return;
            }
            //构建UpdateTask
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, groupName, clusters));
            futureMap.put(serviceKey, future);
        }
    }
    
    private synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        //定时任务延迟一秒执行
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Stop to schedule update if contain task.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void stopUpdateIfContain(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (!futureMap.containsKey(serviceKey)) {
            return;
        }
        synchronized (futureMap) {
            if (!futureMap.containsKey(serviceKey)) {
                return;
            }
            futureMap.remove(serviceKey);
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    /**
     * 1、每个服务对应一个UpdateTask线程，执行各自对应服务的信息更新，首次延时1s执行
     * 2、动态计算线程延时执行时间，在每次执行完后计算下次执行的时间，执行时间公式：
     * Math.min(delayTime << failCount, DEFAULT_DELAY * 60)，即delayTime*2的failCount次方与60*1000的最小值，单位毫秒
     */
    public class UpdateTask implements Runnable {
        
        long lastRefTime = Long.MAX_VALUE;
        
        private boolean isCancel;
        
        private final String serviceName;
        
        private final String groupName;
        
        private final String clusters;
        
        private final String groupedServiceName;
        
        private final String serviceKey;
        
        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;
        
        public UpdateTask(String serviceName, String groupName, String clusters) {
            this.serviceName = serviceName;
            this.groupName = groupName;
            this.clusters = clusters;
            //groupName@@serviceName
            this.groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
            //groupName@@serviceName@@clusters
            this.serviceKey = ServiceInfo.getKey(groupedServiceName, clusters);
        }
        
        @Override
        public void run() {
            //线程执行的延时时间，线程执行完之后在finally块指定当前线程下执行的延时时间
            long delayTime = DEFAULT_DELAY;
            
            try {
                //1、在订阅者（InstancesChangeNotifier）内，如果当前任务对应的服务存在订阅（InstancesChangeNotifier内部的事件监听
                // EventLister集合存在该服务）
                //2、任务对应的ScheduleFuture任务已创建
                if (!changeNotifier.isSubscribed(groupName, serviceName, clusters) && !futureMap.containsKey(
                        serviceKey)) {
                    NAMING_LOGGER.info("update task is stopped, service:{}, clusters:{}", groupedServiceName, clusters);
                    isCancel = true;
                    return;
                }
                // 获取缓存的service信息
                ServiceInfo serviceObj = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
                // 如果本地不存在该服务，那么远程调用获取服务并加入本地缓存；
                if (serviceObj == null) {
                    // 根据serviceName从注册中心服务端获取Service信息
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    // 处理本地缓存
                    serviceInfoHolder.processServiceInfo(serviceObj);
                    lastRefTime = serviceObj.getLastRefTime();
                    return;
                }
                // 如果本地服务的最新时间小于等于等于该任务上次服务的最新时间lastRefTime，那么也重新远程获取服务并加入本地缓存；
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    // 处理本地缓存
                    serviceInfoHolder.processServiceInfo(serviceObj);
                }
                //更新当前任务的最新更新事件lastRefTime;
                lastRefTime = serviceObj.getLastRefTime();
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                // TODO multiple time can be configured.
                // 计算延时时间（下次任务执行时间）：默认是服务的cacheMillis（1s）*6，发生失败时
                delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                // 重置失败数量为0(可能会出现失败情况，没有ServiceInfo，连接失败)
                resetFailCount();
            } catch (NacosException e) {
                handleNacosException(e);
            } catch (Throwable e) {
                handleUnknownException(e);
            } finally {
                if (!isCancel) {
                    // 下次调度刷新时间，下次执行的时间与failCount有关，failCount=0，则下次调度时间为6秒，最长为1分钟
                    // 即当无异常情况下缓存实例的刷新时间是6秒
                    executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60),
                            TimeUnit.MILLISECONDS);
                }
            }
        }
        
        private void handleNacosException(NacosException e) {
            incFailCount();
            int errorCode = e.getErrCode();
            if (NacosException.SERVER_ERROR == errorCode) {
                handleUnknownException(e);
            }
            NAMING_LOGGER.warn("Can't update serviceName: {}, reason: {}", groupedServiceName, e.getErrMsg());
        }
        
        private void handleUnknownException(Throwable throwable) {
            incFailCount();
            NAMING_LOGGER.warn("[NA] failed to update serviceName: {}", groupedServiceName, throwable);
        }
        
        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }
        
        private void resetFailCount() {
            failCount = 0;
        }
    }
}
