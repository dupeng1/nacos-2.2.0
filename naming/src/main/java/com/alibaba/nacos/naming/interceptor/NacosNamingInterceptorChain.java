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

/**
 * Nacos naming interceptor chain.
 *
 * @author xiweng.yy
 */

/**
 * 拦截器链，拦截器链用于存储并管理多个拦截器
 * @param <T>
 */
public interface NacosNamingInterceptorChain<T extends Interceptable> {
    
    /**
     * Add interceptor.
     *
     * @param interceptor interceptor
     */
    //添加指定类型的拦截器对象
    void addInterceptor(NacosNamingInterceptor<T> interceptor);
    
    /**
     * Do intercept by added interceptors.
     *
     * @param object be interceptor object
     */
    //执行拦截的业务操作
    void doInterceptor(T object);
}
