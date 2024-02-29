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

package com.alibaba.nacos.client.naming.remote.gprc;

import com.alibaba.nacos.api.naming.remote.request.NotifySubscriberRequest;
import com.alibaba.nacos.api.naming.remote.response.NotifySubscriberResponse;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.common.remote.client.ServerRequestHandler;

/**
 * Naming push request handler.
 *
 * @author xiweng.yy
 */

/**
 * 当服务端发起NotifySubscriberRequest时返回数据流时，在客户端的双向流的StreamObserver的接收通道的onNext方法里面收到这个数据流时就会交给
 * ServerRequestHandler实例对象（在客户端的默认处理器就是NamingPushRequestHandler）来更新订阅服务在本地的服务信息；
 */
public class NamingPushRequestHandler implements ServerRequestHandler {
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    public NamingPushRequestHandler(ServiceInfoHolder serviceInfoHolder) {
        this.serviceInfoHolder = serviceInfoHolder;
    }
    
    @Override
    public Response requestReply(Request request) {
        if (request instanceof NotifySubscriberRequest) {
            //如果服务端返回的请求是NotifySubscriberRequest，那么就会执行本地服务的更新（就是客户端订阅的服务监听来更新本地服务实例）
            NotifySubscriberRequest notifyRequest = (NotifySubscriberRequest) request;
            serviceInfoHolder.processServiceInfo(notifyRequest.getServiceInfo());
            return new NotifySubscriberResponse();
        }
        return null;
    }
}
