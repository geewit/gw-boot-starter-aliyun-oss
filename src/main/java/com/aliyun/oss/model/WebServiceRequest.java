/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.oss.model;

import com.aliyun.oss.event.ProgressListener;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class WebServiceRequest {
    
    public static final WebServiceRequest NOOP = new WebServiceRequest() { };
    
    private ProgressListener progressListener = ProgressListener.NOOP;
    
    private Map<String, String> parameters = new LinkedHashMap<>();
    private Map<String, String> headers = new LinkedHashMap<>();
    
    public void setProgressListener(ProgressListener progressListener) {
        this.progressListener = (progressListener == null) ? 
                ProgressListener.NOOP : progressListener;
    }

    public ProgressListener getProgressListener() {
        return progressListener;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }
    
    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }
    
    public void addHeader(String key, String value) {
        this.headers.put(key, value);
    }
}
