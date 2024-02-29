/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.healthcheck.v2;

import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.metadata.ClusterMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.NacosHealthCheckTask;
import com.alibaba.nacos.naming.healthcheck.v2.processor.HealthCheckProcessorV2Delegate;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.common.utils.RandomUtils;

import java.util.Optional;

/**
 * Health check task for v2.x.
 *
 * <p>Current health check logic is same as v1.x. TODO refactor health check for v2.x.
 *
 * @author nacos
 */

/**
 * 健康检查任务
 * 在基于IPPort的Client对象中，客户端服务如果是非临时实例（ephemeral=false），那么就会创建这个健康检查任务。
 */
public class HealthCheckTaskV2 extends AbstractExecuteTask implements NacosHealthCheckTask {
    
    private static final int LOWER_CHECK_RT = 2000;
    
    private static final int UPPER_RANDOM_CHECK_RT = 5000;
    
    private static SwitchDomain switchDomain;
    
    private static NamingMetadataManager metadataManager;

    /**
     * 一个客户端对象（此客户端代表提供服务用于被应用访问的客户端）
     * 从这里可以看出，启动一个健康检查任务是以客户端为维度的
     */
    private final IpPortBasedClient client;
    
    private final String taskId;
    
    private long checkRtNormalized = -1;
    /**
     * 检查最佳响应时间
     */
    private long checkRtBest = -1;
    /**
     * 检查最差响应时间
     */
    private long checkRtWorst = -1;
    /**
     * 检查上次响应时间
     */
    private long checkRtLast = -1;
    /**
     * 检查上上次响应时间
     */
    private long checkRtLastLast = -1;
    /**
     * 开始时间
     */
    private long startTime;
    /**
     * 任务是否取消
     */
    private volatile boolean cancelled = false;
    
    public HealthCheckTaskV2(IpPortBasedClient client) {
        this.client = client;
        //设置任务taskId，取IpPortBasedClient的responsibleId，也就是ip:port
        this.taskId = client.getResponsibleId();
    }
    
    private void initIfNecessary() {
        if (switchDomain == null) {
            switchDomain = ApplicationUtils.getBean(SwitchDomain.class);
        }
        if (metadataManager == null) {
            metadataManager = ApplicationUtils.getBean(NamingMetadataManager.class);
        }
        // 初始化响应时间检查
        initCheckRT();
    }

    /**
     * 初始化响应时间值
     */
    private void initCheckRT() {
        if (-1 != checkRtNormalized) {
            return;
        }
        // first check time delay
        //当前线程首次取0～5000之间一个随机数+2000作为线程执行的间隔时间(checkRtNormalized)，单位ms；
        if (null != switchDomain) {
            checkRtNormalized = LOWER_CHECK_RT + RandomUtils.nextInt(0, RandomUtils.nextInt(0, switchDomain.getTcpHealthParams().getMax()));
        } else {
            checkRtNormalized = LOWER_CHECK_RT + RandomUtils.nextInt(0, UPPER_RANDOM_CHECK_RT);
        }
        // 最佳响应时间
        checkRtBest = Long.MAX_VALUE;
        // 最差响应时间为0
        checkRtWorst = 0L;
    }
    
    public IpPortBasedClient getClient() {
        return client;
    }
    
    @Override
    public String getTaskId() {
        return taskId;
    }

    /**
     * 开始执行健康检查任务
     */
    @Override
    public void doHealthCheck() {
        try {
            initIfNecessary();
            //获取所有客户端注册的服务，循环每个服务，在每个循环体内，查找服务的发布信息对应的集群元数据信息，作为健康检查的参数
            for (Service each : client.getAllPublishedService()) {
                // 只有当Service开启了健康检查才执行
                if (switchDomain.isHealthCheckEnabled(each.getGroupedServiceName())) {
                    // 获取Service对应的InstancePublishInfo
                    InstancePublishInfo instancePublishInfo = client.getInstancePublishInfo(each);
                    // 获取集群元数据
                    ClusterMetadata metadata = getClusterMetadata(each, instancePublishInfo);
                    //健康检查的处理交给HealthCheckProcessorV2Delegate。
                    //HealthCheckProcessorV2Delegate是健康检查的处理器代理，在这里默认就是交给TcpHealthCheckProcessor来处理；（
                    ApplicationUtils.getBean(HealthCheckProcessorV2Delegate.class).process(this, each, metadata);
                    if (Loggers.EVT_LOG.isDebugEnabled()) {
                        Loggers.EVT_LOG.debug("[HEALTH-CHECK] schedule health check task: {}", client.getClientId());
                    }
                }
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[HEALTH-CHECK] error while process health check for {}", client.getClientId(), e);
        } finally {
            if (!cancelled) {
                initCheckRT();
                //在方法结束时，向线程池重新提交任务（间隔上面的时间）；
                HealthCheckReactor.scheduleCheck(this);
                // worst == 0 means never checked
                if (this.getCheckRtWorst() > 0) {
                    // TLog doesn't support float so we must convert it into long
                    long checkRtLastLast = getCheckRtLastLast();
                    this.setCheckRtLastLast(this.getCheckRtLast());
                    if (checkRtLastLast > 0) {
                        long diff = ((this.getCheckRtLast() - this.getCheckRtLastLast()) * 10000) / checkRtLastLast;
                        if (Loggers.CHECK_RT.isDebugEnabled()) {
                            Loggers.CHECK_RT.debug("{}->normalized: {}, worst: {}, best: {}, last: {}, diff: {}",
                                    client.getClientId(), this.getCheckRtNormalized(), this.getCheckRtWorst(),
                                    this.getCheckRtBest(), this.getCheckRtLast(), diff);
                        }
                    }
                }
            }
        }
    }
    
    @Override
    public void passIntercept() {
        doHealthCheck();
    }
    
    @Override
    public void afterIntercept() {
        if (!cancelled) {
            try {
                initIfNecessary();
            } finally {
                initCheckRT();
                HealthCheckReactor.scheduleCheck(this);
            }
        }
    }
    
    @Override
    public void run() {
        doHealthCheck();
    }

    /**
     * 获取集群元数据
     * @param service               服务信息
     * @param instancePublishInfo   服务对应的ip等信息
     * @return
     */
    private ClusterMetadata getClusterMetadata(Service service, InstancePublishInfo instancePublishInfo) {
        Optional<ServiceMetadata> serviceMetadata = metadataManager.getServiceMetadata(service);
        if (!serviceMetadata.isPresent()) {
            return new ClusterMetadata();
        }
        String cluster = instancePublishInfo.getCluster();
        ClusterMetadata result = serviceMetadata.get().getClusters().get(cluster);
        return null == result ? new ClusterMetadata() : result;
    }
    
    public long getCheckRtNormalized() {
        return checkRtNormalized;
    }
    
    public long getCheckRtBest() {
        return checkRtBest;
    }
    
    public long getCheckRtWorst() {
        return checkRtWorst;
    }
    
    public void setCheckRtWorst(long checkRtWorst) {
        this.checkRtWorst = checkRtWorst;
    }
    
    public void setCheckRtBest(long checkRtBest) {
        this.checkRtBest = checkRtBest;
    }
    
    public void setCheckRtNormalized(long checkRtNormalized) {
        this.checkRtNormalized = checkRtNormalized;
    }
    
    public boolean isCancelled() {
        return cancelled;
    }
    
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public long getCheckRtLast() {
        return checkRtLast;
    }
    
    public void setCheckRtLast(long checkRtLast) {
        this.checkRtLast = checkRtLast;
    }
    
    public long getCheckRtLastLast() {
        return checkRtLastLast;
    }
    
    public void setCheckRtLastLast(long checkRtLastLast) {
        this.checkRtLastLast = checkRtLastLast;
    }
}
