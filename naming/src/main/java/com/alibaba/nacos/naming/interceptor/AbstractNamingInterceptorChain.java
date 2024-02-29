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

package com.alibaba.nacos.naming.interceptor;

import com.alibaba.nacos.common.spi.NacosServiceLoader;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract Naming Interceptor Chain.
 *
 * @author xiweng.yy
 */
public abstract class AbstractNamingInterceptorChain<T extends Interceptable>
        implements NacosNamingInterceptorChain<T> {

    // 存储多个拦截器
    private final List<NacosNamingInterceptor<T>> interceptors;

    // 限制使用范围为当前包或者其子类
    protected AbstractNamingInterceptorChain(Class<? extends NacosNamingInterceptor<T>> clazz) {
        this.interceptors = new LinkedList<>();
        // 使用SPI模式加载指定的拦截器类型
        // 而且NacosNamingInterceptor内部有判断它需要拦截对象的类型，因此非常灵活
        interceptors.addAll(NacosServiceLoader.load(clazz));
        // 对拦截器的顺序进行排序
        interceptors.sort(Comparator.comparingInt(NacosNamingInterceptor::order));
    }
    
    /**
     * Get all interceptors.
     *
     * @return interceptors list
     */
    protected List<NacosNamingInterceptor<T>> getInterceptors() {
        return interceptors;
    }
    
    @Override
    public void addInterceptor(NacosNamingInterceptor<T> interceptor) {
        // 若手动添加，则需要再次进行排序
        interceptors.add(interceptor);
        interceptors.sort(Comparator.comparingInt(NacosNamingInterceptor::order));
    }
    
    @Override
    public void doInterceptor(T object) {
        // 因为内部的拦截器已经排序过了，所以直接遍历
        for (NacosNamingInterceptor<T> each : interceptors) {
            // 若当前拦截的对象不是当前拦截器所要处理的类型则跳过
            if (!each.isInterceptType(object.getClass())) {
                continue;
            }
            // 执行拦截操作成功之后，继续执行拦截后操作
            if (each.intercept(object)) {
                //成功后，调用被拦截对象的afterIntercept()方法
                object.afterIntercept();
                return;
            }
        }
        // 未拦截的操作
        object.passIntercept();
    }
}
