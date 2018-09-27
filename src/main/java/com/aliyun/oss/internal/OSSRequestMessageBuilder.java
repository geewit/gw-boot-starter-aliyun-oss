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

package com.aliyun.oss.internal;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.common.comm.RequestMessage;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.common.comm.io.FixedLengthInputStream;
import com.aliyun.oss.common.utils.DateUtil;
import com.aliyun.oss.model.WebServiceRequest;

import java.io.InputStream;
import java.net.URI;
import java.util.*;

import static com.aliyun.oss.common.utils.CodingUtils.assertParameterInRange;
import static com.aliyun.oss.internal.OSSConstants.DEFAULT_FILE_SIZE_LIMIT;
import static com.aliyun.oss.internal.OSSUtils.determineFinalEndpoint;
import static com.aliyun.oss.internal.OSSUtils.determineResourcePath;

/*
 * HTTP request message builder.
 */
public class OSSRequestMessageBuilder {
    
    private URI endpoint;
    private HttpMethod method = HttpMethod.GET;
    private String bucket;
    private String key;
    private Map<String, String> headers = new HashMap<>();
    private Map<String, String> parameters = new LinkedHashMap<>();
    private InputStream inputStream;
    private long inputSize = 0; 
    private ServiceClient innerClient;
    private boolean useChunkEncoding = false;
    
    private WebServiceRequest originalRequest;
    
    OSSRequestMessageBuilder(ServiceClient innerClient) {
        this.innerClient = innerClient;
    }
    
    public URI getEndpoint() {
        return endpoint;
    }
    
    public OSSRequestMessageBuilder setEndpoint(URI endpoint) {
        this.endpoint = endpoint;
        return this;
    }
    
    public HttpMethod getMethod() {
        return method;
    }
    
    public OSSRequestMessageBuilder setMethod(HttpMethod method) {
        this.method = method;
        return this;
    }
    
    public String getBucket() {
        return bucket;
    }
    
    public OSSRequestMessageBuilder setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }
    
    public String getKey() {
        return key;
    }
    
    public OSSRequestMessageBuilder setKey(String key) {
        this.key = key;
        return this;
    }
    
    public Map<String, String>getHeaders() {
        return Collections.unmodifiableMap(headers);
    }
    
    public OSSRequestMessageBuilder setHeaders(Map<String, String> headers) {
        this.headers = headers;
        return this;
    }
    
    public OSSRequestMessageBuilder addHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }
    
    public OSSRequestMessageBuilder setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
        return this;
    }
    
    OSSRequestMessageBuilder setInputStream(InputStream instream) {
        this.inputStream = instream;
        return this;
    }
    
    OSSRequestMessageBuilder setInputStreamWithLength(FixedLengthInputStream instream) {
        assertParameterInRange(inputSize, -1, DEFAULT_FILE_SIZE_LIMIT);
        this.inputStream = instream;
        this.inputSize = instream.getLength();
        return this;
    }
    
    OSSRequestMessageBuilder setInputSize(long inputSize) {
        assertParameterInRange(inputSize, -1, DEFAULT_FILE_SIZE_LIMIT);
        this.inputSize = inputSize;
        return this;
    }
    
    OSSRequestMessageBuilder setUseChunkEncoding(boolean useChunkEncoding) {
        this.useChunkEncoding = useChunkEncoding;
        return this;
    }
    
    OSSRequestMessageBuilder setOriginalRequest(WebServiceRequest originalRequest) {
        this.originalRequest = originalRequest;
        return this;
    }

    public RequestMessage build() {       
        Map<String, String> sentHeaders = new HashMap<>(this.headers);
        sentHeaders.put(OSSHeaders.DATE, DateUtil.formatRfc822Date(new Date()));        
        Map<String, String> sentParameters = new LinkedHashMap<>(this.parameters);
        
        RequestMessage request = new RequestMessage(this.originalRequest);
        ClientConfiguration clientCofig = this.innerClient.getClientConfiguration();
        request.setEndpoint(determineFinalEndpoint(this.endpoint, this.bucket, clientCofig));
        request.setResourcePath(determineResourcePath(this.bucket, this.key, clientCofig.isSLDEnabled()));
        request.setHeaders(sentHeaders);
        request.setParameters(sentParameters);
        request.setMethod(this.method);
        request.setContent(this.inputStream);
        request.setContentLength(this.inputSize);
        request.setUseChunkEncoding(this.inputSize == -1 || this.useChunkEncoding);
        
        return request;
    }
   
}
