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

package com.alibaba.nacos.naming.core.v2.client.impl;

import com.alibaba.nacos.naming.core.v2.client.AbstractClient;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.heartbeat.ClientBeatCheckTaskV2;
import com.alibaba.nacos.naming.healthcheck.v2.HealthCheckTaskV2;
import com.alibaba.nacos.naming.misc.ClientConfig;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;

import java.util.Collection;

/**
 * Nacos naming client based ip and port.
 *
 * <p>The client is bind to the ip and port users registered. It's a abstract content to simulate the tcp session
 * client.
 *
 * @author xiweng.yy
 */

/**
 * 基于IP+Port的客户端定义
 */
public class IpPortBasedClient extends AbstractClient {
    
    public static final String ID_DELIMITER = "#";
    //客户端唯一标识，默认格式：ip:port#ephemeral
    private final String clientId;
    //是否临时客户端
    private final boolean ephemeral;
    //表示当前客户端的负责任的标识，在这里，默认responsibleId的值就是从clientId中取#之前的字符串
    private final String responsibleId;
    //客户端心跳检查任务ClientBeatCheckTaskV2，是一个线程
    private ClientBeatCheckTaskV2 beatCheckTask;
    //客户端健康检车任务HealthCheckTaskV2，也是一个线程
    private HealthCheckTaskV2 healthCheckTaskV2;
    
    public IpPortBasedClient(String clientId, boolean ephemeral) {
        this(clientId, ephemeral, null);
    }
    
    public IpPortBasedClient(String clientId, boolean ephemeral, Long revision) {
        super(revision);
        this.ephemeral = ephemeral;
        this.clientId = clientId;
        this.responsibleId = getResponsibleTagFromId();
    }
    
    private String getResponsibleTagFromId() {
        int index = clientId.indexOf(IpPortBasedClient.ID_DELIMITER);
        return clientId.substring(0, index);
    }
    
    public static String getClientId(String address, boolean ephemeral) {
        return address + ID_DELIMITER + ephemeral;
    }
    
    @Override
    public String getClientId() {
        return clientId;
    }
    
    @Override
    public boolean isEphemeral() {
        return ephemeral;
    }
    
    public String getResponsibleId() {
        return responsibleId;
    }

    //重写了方法添加客户端服务注册信息addServiceInstance
    //主要是在调用父类addServiceInstance方法前对参数InstancePublishInfo调用parseToHealthCheckInstance进行类型转化，
    // 转化为HealthCheckInstancePublishInfo
    @Override
    public boolean addServiceInstance(Service service, InstancePublishInfo instancePublishInfo) {
        return super.addServiceInstance(service, parseToHealthCheckInstance(instancePublishInfo));
    }

    /**
     * 1）ephemeral=true，默认就是true
     *
     * 2）所有客户端注册的服务集合publishers是空（也就是没有客户端注册信息）；
     *
     * 3）当前时间与客户端最近更新时间之差大于客户端过期时间（默认3m）；
     * @param currentTime unified current timestamp
     * @return
     */
    @Override
    public boolean isExpire(long currentTime) {
        return isEphemeral() && getAllPublishedService().isEmpty() && currentTime - getLastUpdatedTime() > ClientConfig
                .getInstance().getClientExpiredTime();
    }
    
    public Collection<InstancePublishInfo> getAllInstancePublishInfo() {
        return publishers.values();
    }
    
    @Override
    public void release() {
        super.release();
        if (ephemeral) {
            HealthCheckReactor.cancelCheck(beatCheckTask);
        } else {
            healthCheckTaskV2.setCancelled(true);
        }
    }

    //该方法将入参InstancePublishInfo对象转化为类HealthCheckInstancePublishInfo返回
    //HealthCheckInstancePublishInfo在InstancePublishInfo的基础上增加了健康检查的机制：
    private HealthCheckInstancePublishInfo parseToHealthCheckInstance(InstancePublishInfo instancePublishInfo) {
        HealthCheckInstancePublishInfo result;
        if (instancePublishInfo instanceof HealthCheckInstancePublishInfo) {
            result = (HealthCheckInstancePublishInfo) instancePublishInfo;
        } else {
            result = new HealthCheckInstancePublishInfo();
            result.setIp(instancePublishInfo.getIp());
            result.setPort(instancePublishInfo.getPort());
            result.setHealthy(instancePublishInfo.isHealthy());
            result.setCluster(instancePublishInfo.getCluster());
            result.setExtendDatum(instancePublishInfo.getExtendDatum());
        }
        if (!ephemeral) {
            result.initHealthCheck();
        }
        return result;
    }
    
    /**
     * Init client.
     */
    public void init() {
        if (ephemeral) {
            //针对临时客户端，创建任务ClientBeatCheckTaskV2，交给线程池管理，间隔5s秒执行；
            // 所以对于默认情况下，是通过这里的Client心跳检查来验证客户端是否健康的
            beatCheckTask = new ClientBeatCheckTaskV2(this);
            HealthCheckReactor.scheduleCheck(beatCheckTask);
        } else {
            //针对非临时客户端，创建任务HealthCheckTaskV2，交给线程池管理，延迟2s+(0~5s一个随机数)时间，
            // 这个时间首次使用后，后面的间隔时长都是一致的。
            healthCheckTaskV2 = new HealthCheckTaskV2(this);
            HealthCheckReactor.scheduleCheck(healthCheckTaskV2);
        }
    }
    
    /**
     * Purely put instance into service without publish events.
     */
    public void putServiceInstance(Service service, InstancePublishInfo instance) {
        if (null == publishers.put(service, parseToHealthCheckInstance(instance))) {
            MetricsMonitor.incrementInstanceCount();
        }
    }
}
