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

package com.alibaba.nacos.api.naming.pojo;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Instance.
 *
 * @author nkorange
 */
@JsonInclude(Include.NON_NULL)
public class Instance implements Serializable {
    
    private static final long serialVersionUID = -742906310567291979L;
    
    /**
     * unique id of this instance.
     */
    //实例的唯一ID
    private String instanceId;
    
    /**
     * instance ip.
     */
    //实例IP，提供给消费者进行通信的地址
    private String ip;
    
    /**
     * instance port.
     */
    //端口，提供给消费者访问的端口
    private int port;
    
    /**
     * instance weight.
     */
    //权重，当前实例的权限，浮点类型（默认1.0D）
    private double weight = 1.0D;
    
    /**
     * instance health status.
     */
    //健康状况，默认true
    private boolean healthy = true;
    
    /**
     * If instance is enabled to accept request.
     */
    //实例是否准备好接收请求，默认true
    private boolean enabled = true;
    
    /**
     * If instance is ephemeral.
     *
     * @since 1.0.0
     */
    //实例是否为瞬时的，默认为true
    private boolean ephemeral = true;
    
    /**
     * cluster information of instance.
     */
    //实例所属的集群名称
    private String clusterName;
    
    /**
     * Service information of instance.
     */
    //实例的服务信息
    private String serviceName;
    
    /**
     * user extended attributes.
     */
    //metadata属性
    private Map<String, String> metadata = new HashMap<>();
    
    public String getInstanceId() {
        return this.instanceId;
    }
    
    public void setInstanceId(final String instanceId) {
        this.instanceId = instanceId;
    }
    
    public String getIp() {
        return this.ip;
    }
    
    public void setIp(final String ip) {
        this.ip = ip;
    }
    
    public int getPort() {
        return this.port;
    }
    
    public void setPort(final int port) {
        this.port = port;
    }
    
    public double getWeight() {
        return this.weight;
    }
    
    public void setWeight(final double weight) {
        this.weight = weight;
    }
    
    public boolean isHealthy() {
        return this.healthy;
    }
    
    public void setHealthy(final boolean healthy) {
        this.healthy = healthy;
    }
    
    public String getClusterName() {
        return this.clusterName;
    }
    
    public void setClusterName(final String clusterName) {
        this.clusterName = clusterName;
    }
    
    public String getServiceName() {
        return this.serviceName;
    }
    
    public void setServiceName(final String serviceName) {
        this.serviceName = serviceName;
    }
    
    public Map<String, String> getMetadata() {
        return this.metadata;
    }
    
    public void setMetadata(final Map<String, String> metadata) {
        this.metadata = metadata;
    }
    
    /**
     * add meta data.
     *
     * @param key   meta data key
     * @param value meta data value
     */
    public void addMetadata(final String key, final String value) {
        if (metadata == null) {
            metadata = new HashMap<>(4);
        }
        metadata.put(key, value);
    }
    
    public boolean isEnabled() {
        return this.enabled;
    }
    
    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isEphemeral() {
        return this.ephemeral;
    }
    
    public void setEphemeral(final boolean ephemeral) {
        this.ephemeral = ephemeral;
    }
    
    @Override
    public String toString() {
        return "Instance{" + "instanceId='" + instanceId + '\'' + ", ip='" + ip + '\'' + ", port=" + port + ", weight="
                + weight + ", healthy=" + healthy + ", enabled=" + enabled + ", ephemeral=" + ephemeral
                + ", clusterName='" + clusterName + '\'' + ", serviceName='" + serviceName + '\'' + ", metadata="
                + metadata + '}';
    }
    
    public String toInetAddr() {
        return ip + ":" + port;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Instance)) {
            return false;
        }
        
        final Instance host = (Instance) obj;
        return Instance.strEquals(host.toString(), toString());
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    private static boolean strEquals(final String str1, final String str2) {
        return str1 == null ? str2 == null : str1.equals(str2);
    }
    
    public long getInstanceHeartBeatInterval() {
        //preserved.heart.beat.interval，心跳间隙的key，默认为5s，也就是默认5秒进行一次心跳
        return getMetaDataByKeyWithDefault(PreservedMetadataKeys.HEART_BEAT_INTERVAL,
                Constants.DEFAULT_HEART_BEAT_INTERVAL);
    }
    
    public long getInstanceHeartBeatTimeOut() {
        //preserved.heart.beat.timeout，心跳超时的key，默认为15s，也就是默认15秒收不到心跳，实例将会标记为不健康
        return getMetaDataByKeyWithDefault(PreservedMetadataKeys.HEART_BEAT_TIMEOUT,
                Constants.DEFAULT_HEART_BEAT_TIMEOUT);
    }
    
    public long getIpDeleteTimeout() {
        //preserved.ip.delete.timeout，实例IP被删除的key，默认为30s，也就是30秒收不到心跳，实例将会被移除
        return getMetaDataByKeyWithDefault(PreservedMetadataKeys.IP_DELETE_TIMEOUT,
                Constants.DEFAULT_IP_DELETE_TIMEOUT);
    }
    
    public String getInstanceIdGenerator() {
        //preserved.instance.id.generator，实例ID生成器key，默认为simple
        return getMetaDataByKeyWithDefault(PreservedMetadataKeys.INSTANCE_ID_GENERATOR,
                Constants.DEFAULT_INSTANCE_ID_GENERATOR);
    }
    
    /**
     * Returns {@code true} if this metadata contains the specified key.
     *
     * @param key metadata key
     * @return {@code true} if this metadata contains the specified key
     */
    public boolean containsMetadata(final String key) {
        if (getMetadata() == null || getMetadata().isEmpty()) {
            return false;
        }
        return getMetadata().containsKey(key);
    }
    
    private long getMetaDataByKeyWithDefault(final String key, final long defaultValue) {
        if (getMetadata() == null || getMetadata().isEmpty()) {
            return defaultValue;
        }
        final String value = getMetadata().get(key);
        if (NamingUtils.isNumber(value)) {
            return Long.parseLong(value);
        }
        return defaultValue;
    }
    
    private String getMetaDataByKeyWithDefault(final String key, final String defaultValue) {
        if (getMetadata() == null || getMetadata().isEmpty()) {
            return defaultValue;
        }
        return getMetadata().get(key);
    }
    
}
