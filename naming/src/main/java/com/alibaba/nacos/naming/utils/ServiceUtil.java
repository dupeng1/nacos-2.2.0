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

package com.alibaba.nacos.naming.utils;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.selector.SelectorManager;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service util.
 *
 * @author xiweng.yy
 */
public final class ServiceUtil {
    
    /**
     * Page service name.
     *
     * @param pageNo         page number
     * @param pageSize       size per page
     * @param serviceNameSet service name set
     * @return service name list by paged
     */
    public static List<String> pageServiceName(int pageNo, int pageSize, Collection<String> serviceNameSet) {
        List<String> result = new ArrayList<>(serviceNameSet);
        int start = (pageNo - 1) * pageSize;
        if (start < 0) {
            start = 0;
        }
        if (start >= result.size()) {
            return Collections.emptyList();
        }
        int end = start + pageSize;
        if (end > result.size()) {
            end = result.size();
        }
        for (int i = start; i < end; i++) {
            String serviceName = result.get(i);
            int indexOfSplitter = serviceName.indexOf(Constants.SERVICE_INFO_SPLITER);
            if (indexOfSplitter > 0) {
                serviceName = serviceName.substring(indexOfSplitter + 2);
            }
            result.set(i, serviceName);
        }
        return result.subList(start, end);
    }
    
    /**
     * Select healthy instance of service info.
     *
     * @param serviceInfo original service info
     * @return new service info
     */
    public static ServiceInfo selectHealthyInstances(ServiceInfo serviceInfo) {
        return selectInstances(serviceInfo, true, false);
    }
    
    /**
     * Select healthy instance of service info.
     *
     * @param serviceInfo original service info
     * @return new service info
     */
    public static ServiceInfo selectEnabledInstances(ServiceInfo serviceInfo) {
        return selectInstances(serviceInfo, false, true);
    }
    
    /**
     * Select instance of service info.
     *
     * @param serviceInfo original service info
     * @param cluster     cluster of instances
     * @return new service info
     */
    public static ServiceInfo selectInstances(ServiceInfo serviceInfo, String cluster) {
        return selectInstances(serviceInfo, cluster, false, false);
    }
    
    /**
     * Select instance of service info.
     *
     * @param serviceInfo original service info
     * @param healthyOnly whether only select instance which healthy
     * @param enableOnly  whether only select instance which enabled
     * @return new service info
     */
    public static ServiceInfo selectInstances(ServiceInfo serviceInfo, boolean healthyOnly, boolean enableOnly) {
        return selectInstances(serviceInfo, StringUtils.EMPTY, healthyOnly, enableOnly);
    }
    
    /**
     * Select instance of service info.
     *
     * @param serviceInfo original service info
     * @param cluster     cluster of instances
     * @param healthyOnly whether only select instance which healthy
     * @return new service info
     */
    public static ServiceInfo selectInstances(ServiceInfo serviceInfo, String cluster, boolean healthyOnly) {
        return selectInstances(serviceInfo, cluster, healthyOnly, false);
    }
    
    /**
     * Select instance of service info.
     *
     * @param serviceInfo original service info
     * @param cluster     cluster of instances
     * @param healthyOnly whether only select instance which healthy
     * @param enableOnly  whether only select instance which enabled
     * @return new service info
     */
    public static ServiceInfo selectInstances(ServiceInfo serviceInfo, String cluster, boolean healthyOnly,
            boolean enableOnly) {
        return doSelectInstances(serviceInfo, cluster, healthyOnly, enableOnly, null);
    }
    
    /**
     * Select instance of service info with healthy protection.
     *
     * @param serviceInfo     original service info
     * @param serviceMetadata service meta info
     * @param subscriber subscriber
     * @return new service info
     */
    public static ServiceInfo selectInstancesWithHealthyProtection(ServiceInfo serviceInfo, ServiceMetadata serviceMetadata, Subscriber subscriber) {
        return selectInstancesWithHealthyProtection(serviceInfo, serviceMetadata, subscriber.getCluster(), false, false, subscriber.getIp());
    }

    /**
     * Select instance of service info with healthy protection.
     *
     * @param serviceInfo     original service info
     * @param serviceMetadata service meta info
     * @param healthyOnly     whether only select instance which healthy
     * @param enableOnly      whether only select instance which enabled
     * @param subscriber subscriber
     * @return new service info
     */
    public static ServiceInfo selectInstancesWithHealthyProtection(ServiceInfo serviceInfo,
            ServiceMetadata serviceMetadata, boolean healthyOnly, boolean enableOnly, Subscriber subscriber) {
        return selectInstancesWithHealthyProtection(serviceInfo, serviceMetadata, subscriber.getCluster(), healthyOnly,
                enableOnly, subscriber.getIp());
    }

    /**
     * Select instance of service info with healthy protection.
     *
     * @param serviceInfo     original service info
     * @param serviceMetadata service meta info
     * @param cluster         cluster of instances
     * @param healthyOnly     whether only select instance which healthy
     * @param enableOnly      whether only select instance which enabled
     * @param subscriberIp subscriber ip address
     * @return new service info
     */
    //根据条件过滤实例列表
    //阈值保护得思路是：如果 健康实例数/总实例数 <  阈值 那就把所有不健康得实例转变为健康的实例，让用户误以为都是健康的时候，
    // 这样不会让所有的请求都打到少量健康的机器而发生服务雪崩。
    // 这个策略非常重要。如果么有阈值保护。很快剩余的健康节点在大流量下都可能会挂掉。
    public static ServiceInfo selectInstancesWithHealthyProtection(ServiceInfo serviceInfo, ServiceMetadata serviceMetadata, String cluster,
            boolean healthyOnly, boolean enableOnly, String subscriberIp) {
        //filteredResult 满足集群名称匹配、可用条件、健康条件 过滤后的实例列表
        //allInstances 满足集群条件和可用条件的实例列表
        //healthyCount 满足集群条件和可用条件 且健康的实例列表
        InstancesFilter filter = (filteredResult, allInstances, healthyCount) -> {
            if (serviceMetadata == null) {
                return;
            }
            allInstances = filteredResult.getHosts();
            int originalTotal = allInstances.size();
            // filter ips using selector
            SelectorManager selectorManager = ApplicationUtils.getBean(SelectorManager.class);
            //交给服务元数据的查询处理器处理
            allInstances = selectorManager.select(serviceMetadata.getSelector(), subscriberIp, allInstances);
            //设置selector过滤后的结果
            filteredResult.setHosts(allInstances);
            
            // will re-compute healthCount
            long newHealthyCount = healthyCount;
            //如果selector成功过滤了记录 重新计算健康数
            if (originalTotal != allInstances.size()) {
                for (com.alibaba.nacos.api.naming.pojo.Instance allInstance : allInstances) {
                    if (allInstance.isHealthy()) {
                        newHealthyCount++;
                    }
                }
            }
            //读取阈值保护值
            float threshold = serviceMetadata.getProtectThreshold();
            if (threshold < 0) {
                threshold = 0F;
            }
            if ((float) newHealthyCount / allInstances.size() <= threshold) {
                //设置触发阈值保护状态为true
                Loggers.SRV_LOG.warn("protect threshold reached, return all ips, service: {}", filteredResult.getName());
                filteredResult.setReachProtectionThreshold(true);
                List<com.alibaba.nacos.api.naming.pojo.Instance> filteredInstances = allInstances.stream()
                        .map(i -> {
                            if (!i.isHealthy()) {
                                //对于不健康的实例我们拷贝出新得实例并设置健康状态为true
                                //这里不能直接修改真实实例得健康属性
                                i = InstanceUtil.deepCopy(i);
                                // set all to `healthy` state to protect
                                i.setHealthy(true);
                            } // else deepcopy is unnecessary
                            return i;
                        })
                        .collect(Collectors.toCollection(LinkedList::new));
                //最后赋值给要返回得对象
                filteredResult.setHosts(filteredInstances);
            }
        };
        return doSelectInstances(serviceInfo, cluster, healthyOnly, enableOnly, filter);
    }

    /**
     * Select instance of service info.
     *
     * @param serviceInfo original service info
     * @param cluster     cluster of instances
     * @param healthyOnly whether only select instance which healthy
     * @param enableOnly  whether only select instance which enabled
     * @param filter      do some other filter operation
     * @return new service info
     */
    private static ServiceInfo doSelectInstances(ServiceInfo serviceInfo, String cluster,
                                                 boolean healthyOnly, boolean enableOnly,
                                                 InstancesFilter filter) {
        ServiceInfo result = new ServiceInfo();
        result.setName(serviceInfo.getName());
        result.setGroupName(serviceInfo.getGroupName());
        result.setCacheMillis(serviceInfo.getCacheMillis());
        result.setLastRefTime(System.currentTimeMillis());
        result.setClusters(cluster);
        result.setReachProtectionThreshold(false);
        //把集群字符串分割成set 列表
        Set<String> clusterSets = com.alibaba.nacos.common.utils.StringUtils.isNotBlank(cluster) ? new HashSet<>(
                Arrays.asList(cluster.split(","))) : new HashSet<>();
        //统计健康实例数
        long healthyCount = 0L;
        // The instance list won't be modified almost time.
        List<com.alibaba.nacos.api.naming.pojo.Instance> filteredInstances = new LinkedList<>();
        // The instance list of all filtered by cluster/enabled condition.
        //过滤后的实例列表
        List<com.alibaba.nacos.api.naming.pojo.Instance> allInstances = new LinkedList<>();
        for (com.alibaba.nacos.api.naming.pojo.Instance ip : serviceInfo.getHosts()) {
            //clusterSets是否包含ip所属的集群
            //如果enableOnly为true代表实例要求可用否则 允许返回不可用的实例
            if (checkCluster(clusterSets, ip) && checkEnabled(enableOnly, ip)) {
                //如果healthyOnly 为true代表实例要求健康否则 允许返回不健康的实例
                if (!healthyOnly || ip.isHealthy()) {
                    //healthyOnly  为true时 filteredInstances只包括健康实例
                    //healthyOnly  为false时 filteredInstances包括所有实例
                    //此时filteredInstances的记录跟allInstances记录一致
                    filteredInstances.add(ip);
                }
                //统计健康的实例数
                if (ip.isHealthy()) {
                    healthyCount += 1;
                }
                //记录所有健康不健康的实例列表
                allInstances.add(ip);
            }
        }
        //设置过滤后的实例列表
        result.setHosts(filteredInstances);
        if (filter != null) {
            //交给过滤器去过滤一遍
            filter.doFilter(result, allInstances, healthyCount);
        }
        return result;
    }
    //运行clusterSets为空代表查询的集群字符串为空 无需校验实例的集群
    private static boolean checkCluster(Set<String> clusterSets, com.alibaba.nacos.api.naming.pojo.Instance instance) {
        if (clusterSets.isEmpty()) {
            return true;
        }
        //包含当前实例的集群则返回true
        return clusterSets.contains(instance.getClusterName());
    }
    
    private static boolean checkEnabled(boolean enableOnly, com.alibaba.nacos.api.naming.pojo.Instance instance) {
        return !enableOnly || instance.isEnabled();
    }

    private interface InstancesFilter {

        /**
         * Do customized filtering.
         *
         * @param filteredResult result with instances already been filtered cluster/enabled/healthy
         * @param allInstances   all instances filtered by cluster/enabled
         * @param healthyCount   healthy instances count filtered by cluster/enabled
         */
        void doFilter(ServiceInfo filteredResult,
                      List<com.alibaba.nacos.api.naming.pojo.Instance> allInstances,
                      long healthyCount);

    }

}
