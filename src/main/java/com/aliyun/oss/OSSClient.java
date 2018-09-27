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

package com.aliyun.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.auth.ServiceSignature;
import com.aliyun.oss.common.comm.*;
import com.aliyun.oss.common.utils.BinaryUtil;
import com.aliyun.oss.common.utils.DateUtil;
import com.aliyun.oss.common.utils.HttpHeaders;
import com.aliyun.oss.common.utils.HttpUtil;
import com.aliyun.oss.internal.*;
import com.aliyun.oss.model.*;
import com.aliyun.oss.model.SetBucketCORSRequest.CORSRule;

import java.io.*;
import java.net.*;
import java.util.*;

import static com.aliyun.oss.common.utils.CodingUtils.assertParameterNotNull;
import static com.aliyun.oss.common.utils.IOUtils.checkFile;
import static com.aliyun.oss.common.utils.LogUtils.logException;
import static com.aliyun.oss.internal.OSSConstants.DEFAULT_CHARSET_NAME;
import static com.aliyun.oss.internal.OSSConstants.DEFAULT_OSS_ENDPOINT;
import static com.aliyun.oss.internal.OSSUtils.*;
import static com.aliyun.oss.internal.RequestParameters.*;

/**
 * 访问阿里云对象存储服务（Object Storage Service， OSS）的入口类。
 */
public class OSSClient implements OSS {

    /* The default credentials provider */
    private CredentialsProvider credsProvider;

    /* The valid endpoint for accessing to OSS services */
    private URI endpoint;

    /* The default service client */
    private ServiceClient serviceClient;

    /* The miscellaneous OSS operations */
    private OSSBucketOperation bucketOperation;
    private OSSObjectOperation objectOperation;
    private OSSMultipartOperation multipartOperation;
    private CORSOperation corsOperation;
    private OSSUploadOperation uploadOperation;
    private OSSDownloadOperation downloadOperation;
    private LiveChannelOperation liveChannelOperation;
    private OSSUdfOperation udfOperation;
    
    /**
     * 使用默认的OSS Endpoint(http://oss-cn-hangzhou.aliyuncs.com)及
     * 阿里云颁发的Access Id/Access Key构造一个新的{@link OSSClient}对象。
     * 
     * @param accessKeyId
     *            访问OSS的Access Key ID。
     * @param secretAccessKey
     *            访问OSS的Secret Access Key。
     */
    @Deprecated
    public OSSClient(String accessKeyId, String secretAccessKey) {
        this(DEFAULT_OSS_ENDPOINT, new DefaultCredentialProvider(accessKeyId, secretAccessKey));
    }

    /**
     * 使用指定的OSS Endpoint、阿里云颁发的Access Id/Access Key构造一个新的{@link OSSClient}对象。
     * 
     * @param endpoint
     *            OSS服务的Endpoint。
     * @param accessKeyId
     *            访问OSS的Access Key ID。
     * @param secretAccessKey
     *            访问OSS的Secret Access Key。
     */
    public OSSClient(String endpoint, String accessKeyId, String secretAccessKey) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, secretAccessKey), null);
    }
    
    /**
     * 使用指定的OSS Endpoint、STS提供的临时Token信息(Access Id/Access Key/Security Token)
     * 构造一个新的{@link OSSClient}对象。
     * 
     * @param endpoint
     *            OSS服务的Endpoint。
     * @param accessKeyId
     *            STS提供的临时访问ID。
     * @param secretAccessKey
     *            STS提供的访问密钥。
     * @param securityToken
     *               STS提供的安全令牌。
     */
    public OSSClient(String endpoint, String accessKeyId, String secretAccessKey, String securityToken) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, secretAccessKey, securityToken), null);
    }
    
    /**
     * 使用指定的OSS Endpoint、阿里云颁发的Access Id/Access Key、客户端配置
     * 构造一个新的{@link OSSClient}对象。
     * 
     * @param endpoint
     *            OSS服务的Endpoint。
     * @param accessKeyId
     *            访问OSS的Access Key ID。
     * @param secretAccessKey
     *            访问OSS的Secret Access Key。
     * @param config
     *            客户端配置 {@link ClientConfiguration}。 如果为null则会使用默认配置。
     */
    public OSSClient(String endpoint, String accessKeyId, String secretAccessKey, 
            ClientConfiguration config) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, secretAccessKey), config);
    }
    
    /**
     * 使用指定的OSS Endpoint、STS提供的临时Token信息(Access Id/Access Key/Security Token)、
     * 客户端配置构造一个新的{@link OSSClient}对象。
     * 
     * @param endpoint
     *            OSS服务的Endpoint。
     * @param accessKeyId
     *            STS提供的临时访问ID。
     * @param secretAccessKey
     *            STS提供的访问密钥。
     * @param securityToken
     *               STS提供的安全令牌。
     * @param config
     *            客户端配置 {@link ClientConfiguration}。 如果为null则会使用默认配置。
     */
    public OSSClient(String endpoint, String accessKeyId, String secretAccessKey, String securityToken, 
            ClientConfiguration config) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, secretAccessKey, securityToken), config);
    }

    /**
     * 使用默认配置及指定的{@link CredentialsProvider}与Endpoint构造一个新的{@link OSSClient}对象。
     * @param endpoint OSS services的Endpoint。
     * @param credsProvider Credentials提供者。
     */
    private OSSClient(String endpoint, CredentialsProvider credsProvider) {
        this(endpoint, credsProvider, null);
    }
    
    /**
     * 使用指定的{@link CredentialsProvider}、配置及Endpoint构造一个新的{@link OSSClient}对象。
     * @param endpoint OSS services的Endpoint。
     * @param credsProvider Credentials提供者。
     * @param config client配置。
     */
    private OSSClient(String endpoint, CredentialsProvider credsProvider, ClientConfiguration config) {
        this.credsProvider = credsProvider;
        config = config == null ? new ClientConfiguration() : config;
        if (config.isRequestTimeoutEnabled()) {
            this.serviceClient = new TimeoutServiceClient(config);
        } else {
            this.serviceClient = new DefaultServiceClient(config);
        }
        initOperations();
        setEndpoint(endpoint);
    }
    
    /**
     * 获取OSS services的Endpoint。
     * @return OSS services的Endpoint。
     */
    public synchronized URI getEndpoint() {
        return URI.create(endpoint.toString());
    }
    
    /**
     * 设置OSS services的Endpoint。
     * @param endpoint OSS services的Endpoint。
     */
    public synchronized void setEndpoint(String endpoint) {
        URI uri = toURI(endpoint);
        this.endpoint = uri;
        
        if (isIpOrLocalhost(uri)) {
            serviceClient.getClientConfiguration().setSLDEnabled(true);
        }
        
        this.bucketOperation.setEndpoint(uri);
        this.objectOperation.setEndpoint(uri);
        this.multipartOperation.setEndpoint(uri);
        this.corsOperation.setEndpoint(uri);
        this.liveChannelOperation.setEndpoint(uri);
        this.udfOperation.setEndpoint(uri);
    }
    
    /**
     * 判定一个网络地址是否是IP还是域名。IP都是用二级域名，域名(Localhost除外)不使用二级域名。
     * @param uri URI。
     */
    private boolean isIpOrLocalhost(URI uri){
        if (uri.getHost().equals("localhost")) {
            return true;
        }
        
        InetAddress ia;
        try {
            ia = InetAddress.getByName(uri.getHost());
        } catch (UnknownHostException e) {
            return false;
        }

        return ia.getHostName().equals(ia.getHostAddress());

    }
    
    private URI toURI(String endpoint) throws IllegalArgumentException {        
        if (!endpoint.contains("://")) {
            ClientConfiguration conf = this.serviceClient.getClientConfiguration();
            endpoint = conf.getProtocol().toString() + "://" + endpoint;
        }

        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    private void initOperations() {
        this.bucketOperation = new OSSBucketOperation(this.serviceClient, this.credsProvider);
        this.objectOperation = new OSSObjectOperation(this.serviceClient, this.credsProvider);
        this.multipartOperation = new OSSMultipartOperation(this.serviceClient, this.credsProvider);
        this.corsOperation = new CORSOperation(this.serviceClient, this.credsProvider);
        this.uploadOperation = new OSSUploadOperation(this.multipartOperation);
        this.downloadOperation = new OSSDownloadOperation(objectOperation);
        this.liveChannelOperation = new LiveChannelOperation(this.serviceClient, this.credsProvider);
        this.udfOperation = new OSSUdfOperation(this.serviceClient, this.credsProvider);
    }
    
    @Override
    public void switchCredentials(Credentials creds) {
        if (creds == null) {
            throw new IllegalArgumentException("creds should not be null.");
        }
        
        this.credsProvider.setCredentials(creds);
    }
    
    public CredentialsProvider getCredentialsProvider() {
        return this.credsProvider;
    }
    
    public ClientConfiguration getClientConfiguration() {
        return serviceClient.getClientConfiguration();
    }

    @Override
    public Bucket createBucket(String bucketName) 
            throws OSSException, ClientException {
        return this.createBucket(new CreateBucketRequest(bucketName));
    }

    @Override
    public Bucket createBucket(CreateBucketRequest createBucketRequest)
            throws OSSException, ClientException {
        return bucketOperation.createBucket(createBucketRequest);
    }

    @Override
    public void deleteBucket(String bucketName) 
            throws OSSException, ClientException {
        this.deleteBucket(new GenericRequest(bucketName));
    }
    
    @Override
    public void deleteBucket(GenericRequest genericRequest)
            throws OSSException, ClientException {
        bucketOperation.deleteBucket(genericRequest);
    }

    @Override
    public List<Bucket> listBuckets() throws OSSException, ClientException {
        return bucketOperation.listBuckets();
    }

    @Override
    public BucketList listBuckets(ListBucketsRequest listBucketsRequest) 
            throws OSSException, ClientException {
        return bucketOperation.listBuckets(listBucketsRequest);
    }

    @Override
    public BucketList listBuckets(String prefix, String marker, Integer maxKeys) 
            throws OSSException, ClientException {
        return bucketOperation.listBuckets(new ListBucketsRequest(prefix, marker, maxKeys));
    }

    @Override
    public void setBucketAcl(String bucketName, CannedAccessControlList cannedACL) 
            throws OSSException, ClientException {
        this.setBucketAcl(new SetBucketAclRequest(bucketName, cannedACL));
    }
    
    @Override
    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
            throws OSSException, ClientException {
        bucketOperation.setBucketAcl(setBucketAclRequest);
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName) 
            throws OSSException, ClientException {
        return this.getBucketAcl(new GenericRequest(bucketName));
    }
    
    @Override
    public AccessControlList getBucketAcl(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return bucketOperation.getBucketAcl(genericRequest);
    }
     
     @Override
     public void setBucketReferer(String bucketName, BucketReferer referer) 
             throws OSSException, ClientException {
         this.setBucketReferer(new SetBucketRefererRequest(bucketName, referer));
     }
     
     @Override
     public void setBucketReferer(SetBucketRefererRequest setBucketRefererRequest)
             throws OSSException, ClientException {
         bucketOperation.setBucketReferer(setBucketRefererRequest);
     }
     
     @Override
     public BucketReferer getBucketReferer(String bucketName)
             throws OSSException, ClientException {
         return this.getBucketReferer(new GenericRequest(bucketName));
     }

     @Override
     public BucketReferer getBucketReferer(GenericRequest genericRequest)
             throws OSSException, ClientException {
         return bucketOperation.getBucketReferer(genericRequest);
     }
          
    @Override
    public String getBucketLocation(String bucketName) 
            throws OSSException, ClientException {
        return this.getBucketLocation(new GenericRequest(bucketName));
    }
    
    @Override
    public String getBucketLocation(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return bucketOperation.getBucketLocation(genericRequest);
    }

    @Override
    public boolean doesBucketExist(String bucketName) 
            throws OSSException, ClientException {
        return this.doesBucketExist(new GenericRequest(bucketName));
    }
    
    @Override
    public boolean doesBucketExist(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return bucketOperation.doesBucketExists(genericRequest);
    }

    /**
     * 已过时。请使用{@link OSSClient#doesBucketExist(String)}。
     */
    @Deprecated
    public boolean isBucketExist(String bucketName) 
            throws OSSException, ClientException {
        return this.doesBucketExist(bucketName);
    }

    @Override
    public ObjectListing listObjects(String bucketName) 
            throws OSSException, ClientException {
        return listObjects(new ListObjectsRequest(bucketName, null, null, null, null));
    }

    @Override
    public ObjectListing listObjects(String bucketName, String prefix) 
            throws OSSException, ClientException {
        return listObjects(new ListObjectsRequest(bucketName, prefix, null, null, null));
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) 
            throws OSSException, ClientException {
        return bucketOperation.listObjects(listObjectsRequest);
    }
    
    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input) 
            throws OSSException, ClientException {
        return putObject(bucketName, key, input, null);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
            throws OSSException, ClientException {
        return putObject(new PutObjectRequest(bucketName, key, input, metadata));
    }
    
    @Override
    public PutObjectResult putObject(String bucketName, String key, File file, ObjectMetadata metadata) 
            throws OSSException, ClientException {
        return putObject(new PutObjectRequest(bucketName, key, file, metadata));
    }
    
    @Override
    public PutObjectResult putObject(String bucketName, String key, File file)
            throws OSSException, ClientException {
        return putObject(bucketName, key, file, null);
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws OSSException, ClientException {
        return objectOperation.putObject(putObjectRequest);
    }
    
    @Override
    public PutObjectResult putObject(URL signedUrl, String filePath, Map<String, String> requestHeaders)
            throws OSSException, ClientException {
        return putObject(signedUrl, filePath, requestHeaders, false);
    }
    
    @Override
    public PutObjectResult putObject(URL signedUrl, String filePath, Map<String, String> requestHeaders,
            boolean useChunkEncoding) throws OSSException, ClientException {
        
        FileInputStream requestContent = null;
        try {
            File toUpload = new File(filePath);
            if (!checkFile(toUpload)) {
                throw new IllegalArgumentException("Illegal file path: " + filePath);
            }
            long fileSize = toUpload.length();
            requestContent = new FileInputStream(toUpload);
            
            return putObject(signedUrl, requestContent, fileSize, requestHeaders, useChunkEncoding);
        } catch (FileNotFoundException e) {
            throw new ClientException(e);
        } finally {
            if (requestContent != null) {
                try {
                    requestContent.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    @Override
    public PutObjectResult putObject(URL signedUrl, InputStream requestContent, long contentLength, 
            Map<String, String> requestHeaders) 
                    throws OSSException, ClientException {
        return putObject(signedUrl, requestContent, contentLength, requestHeaders, false);
    }
    
    @Override
    public PutObjectResult putObject(URL signedUrl, InputStream requestContent, long contentLength,
            Map<String, String> requestHeaders, boolean useChunkEncoding) 
                    throws OSSException, ClientException {
        return objectOperation.putObject(signedUrl, requestContent, contentLength, requestHeaders, useChunkEncoding);
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) 
            throws OSSException, ClientException {
        return copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey));
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) 
            throws OSSException, ClientException {
        return objectOperation.copyObject(copyObjectRequest);
    }

    @Override
    public OSSObject getObject(String bucketName, String key) 
            throws OSSException, ClientException {
        return this.getObject(new GetObjectRequest(bucketName, key));
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File file) 
            throws OSSException, ClientException {
        return objectOperation.getObject(getObjectRequest, file);
    }

    @Override
    public OSSObject getObject(GetObjectRequest getObjectRequest) 
            throws OSSException, ClientException {
        return objectOperation.getObject(getObjectRequest);
    }

    @Override
    public OSSObject getObject(URL signedUrl, Map<String, String> requestHeaders) 
            throws OSSException, ClientException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(signedUrl, requestHeaders);
        return objectOperation.getObject(getObjectRequest);
    }
    
    @Override
    public SimplifiedObjectMeta getSimplifiedObjectMeta(String bucketName, String key)
            throws OSSException, ClientException {
        return this.getSimplifiedObjectMeta(new GenericRequest(bucketName, key));
    }

    @Override
    public SimplifiedObjectMeta getSimplifiedObjectMeta(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return this.objectOperation.getSimplifiedObjectMeta(genericRequest);
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key)
            throws OSSException, ClientException {
        return this.getObjectMetadata(new GenericRequest(bucketName, key));
    }
    
    @Override
    public ObjectMetadata getObjectMetadata(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return objectOperation.getObjectMetadata(genericRequest);
    }
    
    @Override
    public AppendObjectResult appendObject(AppendObjectRequest appendObjectRequest) 
            throws OSSException, ClientException {
        return objectOperation.appendObject(appendObjectRequest);
    }

    @Override
    public void deleteObject(String bucketName, String key) 
            throws OSSException, ClientException {
        this.deleteObject(new GenericRequest(bucketName, key));
    }
    
    @Override
    public void deleteObject(GenericRequest genericRequest)
            throws OSSException, ClientException {
        objectOperation.deleteObject(genericRequest);
    }
    
    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) 
            throws OSSException, ClientException {
        return objectOperation.deleteObjects(deleteObjectsRequest);
    }
    
    @Override
    public boolean doesObjectExist(String bucketName, String key)
            throws OSSException, ClientException {
        return doesObjectExist(new GenericRequest(bucketName, key));
    }
    
    @Override
    public boolean doesObjectExist(String bucketName, String key, boolean isOnlyInOSS) {
        if (isOnlyInOSS) {
            return doesObjectExist(bucketName, key);
        } else {
            return objectOperation.doesObjectExistWithRedirect(bucketName, key);
        }
    }
    
    @Deprecated
    @Override
    public boolean doesObjectExist(HeadObjectRequest headObjectRequest)
            throws OSSException, ClientException {
        return doesObjectExist(new GenericRequest(headObjectRequest.getBucketName(), headObjectRequest.getKey())); 
    }
    
    @Override
    public boolean doesObjectExist(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return objectOperation.doesObjectExist(genericRequest);
    }
    
    @Override
    public void setObjectAcl(String bucketName, String key, CannedAccessControlList cannedACL) 
            throws OSSException, ClientException {
        this.setObjectAcl(new SetObjectAclRequest(bucketName, key, cannedACL));
    }
    
    @Override
    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest)
            throws OSSException, ClientException {
        objectOperation.setObjectAcl(setObjectAclRequest);
    }

    @Override
    public ObjectAcl getObjectAcl(String bucketName, String key)
            throws OSSException, ClientException {
        return this.getObjectAcl(new GenericRequest(bucketName, key));
    }
    
    @Override
    public ObjectAcl getObjectAcl(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return objectOperation.getObjectAcl(genericRequest);
    }
    
    @Override
    public RestoreObjectResult restoreObject(String bucketName, String key)
            throws OSSException, ClientException {
        return this.restoreObject(new GenericRequest(bucketName, key));
    }
    
    @Override
    public RestoreObjectResult restoreObject(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return objectOperation.restoreObject(genericRequest);
    }
    
    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration) 
            throws ClientException {
        return generatePresignedUrl(bucketName, key, expiration, HttpMethod.GET);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
            throws ClientException {
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key);
        request.setExpiration(expiration);
        request.setMethod(method);

        return generatePresignedUrl(request);
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest request) 
            throws ClientException {

        assertParameterNotNull(request, "request");
        
        String bucketName = request.getBucketName();
        if (request.getBucketName() == null) {
            throw new IllegalArgumentException(OSS_RESOURCE_MANAGER.getString("MustSetBucketName"));
        }
        ensureBucketNameValid(request.getBucketName());
        
        if (request.getExpiration() == null) {
            throw new IllegalArgumentException(OSS_RESOURCE_MANAGER.getString("MustSetExpiration"));
        }

        Credentials currentCreds = credsProvider.getCredentials();
        String accessId = currentCreds.getAccessKeyId();
        String accessKey = currentCreds.getSecretAccessKey();
        boolean useSecurityToken = currentCreds.useSecurityToken();
        HttpMethod method = request.getMethod() != null ? request.getMethod() : HttpMethod.GET;

        String expires = String.valueOf(request.getExpiration().getTime() / 1000L);
        String key = request.getKey();
        ClientConfiguration config = serviceClient.getClientConfiguration();
        String resourcePath = OSSUtils.determineResourcePath(bucketName, key, config.isSLDEnabled());

        RequestMessage requestMessage = new RequestMessage();
        requestMessage.setEndpoint(OSSUtils.determineFinalEndpoint(endpoint, bucketName, config));
        requestMessage.setMethod(method);
        requestMessage.setResourcePath(resourcePath);
        requestMessage.setHeaders(request.getHeaders());
        
        requestMessage.addHeader(HttpHeaders.DATE, expires);
        if (request.getContentType() != null && !request.getContentType().trim().equals("")) {
            requestMessage.addHeader(HttpHeaders.CONTENT_TYPE, request.getContentType());
        }
        if (request.getContentMD5() != null && request.getContentMD5().trim().equals("")) {
            requestMessage.addHeader(HttpHeaders.CONTENT_MD5, request.getContentMD5());
        }
        for (Map.Entry<String, String> h : request.getUserMetadata().entrySet()) {
            requestMessage.addHeader(OSSHeaders.OSS_USER_METADATA_PREFIX + h.getKey(), h.getValue());
        }
        
        Map<String, String> responseHeaderParams = new HashMap<>();
        populateResponseHeaderParameters(responseHeaderParams, request.getResponseHeaders());
        if (responseHeaderParams.size() > 0) {
            requestMessage.setParameters(responseHeaderParams);
        }

        if (request.getQueryParameter() != null && request.getQueryParameter().size() > 0) {
            for (Map.Entry<String, String> entry : request.getQueryParameter().entrySet()) {
                requestMessage.addParameter(entry.getKey(), entry.getValue());
            }
        }
        
        if (request.getProcess() != null && !request.getProcess().trim().equals("")) {
        	requestMessage.addParameter(RequestParameters.SUBRESOURCE_PROCESS, request.getProcess());
        }
        
        if (useSecurityToken) {
            requestMessage.addParameter(SECURITY_TOKEN, currentCreds.getSecurityToken());
        }

        String canonicalResource = "/" + ((bucketName != null) ? bucketName : "") 
                + ((key != null ? "/" + key : ""));
        String canonicalString = SignUtils.buildCanonicalString(method.toString(), canonicalResource, 
                requestMessage);
        String signature = ServiceSignature.create().computeSignature(accessKey, canonicalString);

        Map<String, String> params = new LinkedHashMap<>();
        params.put(HttpHeaders.EXPIRES, expires);
        params.put(OSS_ACCESS_KEY_ID, accessId);
        params.put(SIGNATURE, signature);
        params.putAll(requestMessage.getParameters());

        String queryString = HttpUtil.paramToQueryString(params, DEFAULT_CHARSET_NAME);

        /* Compse HTTP request uri. */
        String url = requestMessage.getEndpoint().toString();
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += resourcePath + "?" + queryString;

        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new ClientException(e);
        }
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) 
            throws OSSException, ClientException {
        multipartOperation.abortMultipartUpload(request);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws OSSException, ClientException {
        return multipartOperation.completeMultipartUpload(request);
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws OSSException, ClientException {
        return multipartOperation.initiateMultipartUpload(request);
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) 
            throws OSSException, ClientException {
        return multipartOperation.listMultipartUploads(request);
    }

    @Override
    public PartListing listParts(ListPartsRequest request) 
            throws OSSException, ClientException {
        return multipartOperation.listParts(request);
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) 
            throws OSSException, ClientException {
        return multipartOperation.uploadPart(request);
    }
    
    @Override
    public UploadPartCopyResult uploadPartCopy(UploadPartCopyRequest request) 
            throws OSSException, ClientException {
        return multipartOperation.uploadPartCopy(request);
    }

    @Override
    public void setBucketCORS(SetBucketCORSRequest request) 
            throws OSSException, ClientException {
        corsOperation.setBucketCORS(request);
    }

    @Override
    public List<CORSRule> getBucketCORSRules(String bucketName) 
            throws OSSException, ClientException {
        return this.getBucketCORSRules(new GenericRequest(bucketName));
    }
    
    @Override
    public List<CORSRule> getBucketCORSRules(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return corsOperation.getBucketCORSRules(genericRequest);
    }

    @Override
    public void deleteBucketCORSRules(String bucketName) 
            throws OSSException, ClientException {
        this.deleteBucketCORSRules(new GenericRequest(bucketName));
    }
    
    @Override
    public void deleteBucketCORSRules(GenericRequest genericRequest)
            throws OSSException, ClientException {
        corsOperation.deleteBucketCORS(genericRequest);
    }

    @Override
    public ResponseMessage optionsObject(OptionsRequest request) 
            throws OSSException, ClientException {
        return corsOperation.optionsObject(request);
    }
    
    @Override
    public void setBucketLogging(SetBucketLoggingRequest request) 
            throws OSSException, ClientException {
         bucketOperation.setBucketLogging(request);
    }
    
    @Override
    public BucketLoggingResult getBucketLogging(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketLogging(new GenericRequest(bucketName));
    }
    
    @Override
    public BucketLoggingResult getBucketLogging(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return bucketOperation.getBucketLogging(genericRequest);
    }
    
    @Override
    public void deleteBucketLogging(String bucketName) 
            throws OSSException, ClientException {
        this.deleteBucketLogging(new GenericRequest(bucketName));
    }
    
    @Override
    public void deleteBucketLogging(GenericRequest genericRequest)
            throws OSSException, ClientException {
        bucketOperation.deleteBucketLogging(genericRequest);
    }
    @Override
	public void putBucketImage(PutBucketImageRequest request)
	    		throws OSSException, ClientException{
		bucketOperation.putBucketImage(request);
	}
    
	@Override
	public GetBucketImageResult getBucketImage(String bucketName) 
			throws OSSException, ClientException{
		return bucketOperation.getBucketImage(bucketName, new GenericRequest());
	}
	
	@Override
	public GetBucketImageResult getBucketImage(String bucketName, GenericRequest genericRequest) 
			throws OSSException, ClientException{
		return bucketOperation.getBucketImage(bucketName, genericRequest);
	}
	
	@Override
	public void deleteBucketImage(String bucketName) 
			throws OSSException, ClientException{
		bucketOperation.deleteBucketImage(bucketName, new GenericRequest());
	}
	
	@Override
	public void deleteBucketImage(String bucketName, GenericRequest genericRequest) 
			throws OSSException, ClientException{
		bucketOperation.deleteBucketImage(bucketName, genericRequest);
	}
	
	@Override
	public void putImageStyle(PutImageStyleRequest putImageStyleRequest)
			throws OSSException, ClientException{
		bucketOperation.putImageStyle(putImageStyleRequest);
	}
	
	@Override
	public void deleteImageStyle(String bucketName, String styleName)
			throws OSSException, ClientException{
		bucketOperation.deleteImageStyle(bucketName, styleName, new GenericRequest());
	}
	
	@Override
	public void deleteImageStyle(String bucketName, String styleName, GenericRequest genericRequest)
			throws OSSException, ClientException{
		bucketOperation.deleteImageStyle(bucketName, styleName, genericRequest);
	}
	
	@Override
	public GetImageStyleResult getImageStyle(String bucketName, String styleName)
    		throws OSSException, ClientException{
		return bucketOperation.getImageStyle(bucketName, styleName, new GenericRequest());
	}
	
	@Override
	public GetImageStyleResult getImageStyle(String bucketName, String styleName, GenericRequest genericRequest)
    		throws OSSException, ClientException{
		return bucketOperation.getImageStyle(bucketName, styleName, genericRequest);
	}
	
	@Override
    public List<Style> listImageStyle(String bucketName) 
    		throws OSSException, ClientException {
            return bucketOperation.listImageStyle(bucketName, new GenericRequest());
    }
	
	@Override
    public List<Style> listImageStyle(String bucketName, GenericRequest genericRequest) 
    		throws OSSException, ClientException {
            return bucketOperation.listImageStyle(bucketName, genericRequest);
    }
	
	@Override
    public void setBucketProcess(SetBucketProcessRequest setBucketProcessRequest)
            throws OSSException, ClientException {
	    bucketOperation.setBucketProcess(setBucketProcessRequest);
	}
    
	@Override
    public BucketProcess getBucketProcess(String bucketName)
            throws OSSException, ClientException {
	    return this.getBucketProcess(new GenericRequest(bucketName));
	}
    
	@Override
    public BucketProcess getBucketProcess(GenericRequest genericRequest) 
            throws OSSException, ClientException {
	    return bucketOperation.getBucketProcess(genericRequest);
	}

    @Override
    public void setBucketWebsite(SetBucketWebsiteRequest setBucketWebSiteRequest)
            throws OSSException, ClientException {
        bucketOperation.setBucketWebsite(setBucketWebSiteRequest);
    }

    @Override
    public BucketWebsiteResult getBucketWebsite(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketWebsite(new GenericRequest(bucketName));
    }
    
    @Override
    public BucketWebsiteResult getBucketWebsite(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return bucketOperation.getBucketWebsite(genericRequest);
    }

    @Override
    public void deleteBucketWebsite(String bucketName) 
            throws OSSException, ClientException {
        this.deleteBucketWebsite(new GenericRequest(bucketName));
    }
    
    @Override
    public void deleteBucketWebsite(GenericRequest genericRequest)
            throws OSSException, ClientException {
        bucketOperation.deleteBucketWebsite(genericRequest);
    }
    
    @Override
    public String generatePostPolicy(Date expiration, PolicyConditions conds) {
        String formatedExpiration = DateUtil.formatIso8601Date(expiration);
        String jsonizedExpiration = String.format("\"expiration\":\"%s\"", formatedExpiration);
        String jsonizedConds = conds.jsonize();

        return String.format("{%s,%s}", jsonizedExpiration, jsonizedConds);
    }
    
    @Override
    public String calculatePostSignature(String postPolicy) throws ClientException {
        try {
            byte[] binaryData = postPolicy.getBytes(DEFAULT_CHARSET_NAME);
            String encPolicy = BinaryUtil.toBase64String(binaryData);
            return ServiceSignature.create().computeSignature(
                    credsProvider.getCredentials().getSecretAccessKey(), encPolicy);
        } catch (UnsupportedEncodingException ex) {
            throw new ClientException("Unsupported charset: " + ex.getMessage());
        }
    }
    
    @Override
    public void setBucketLifecycle(SetBucketLifecycleRequest setBucketLifecycleRequest)
            throws OSSException, ClientException {
        bucketOperation.setBucketLifecycle(setBucketLifecycleRequest);
    }
    
    @Override
    public List<LifecycleRule> getBucketLifecycle(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketLifecycle(new GenericRequest(bucketName));
    }
    
    @Override
    public List<LifecycleRule> getBucketLifecycle(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return bucketOperation.getBucketLifecycle(genericRequest);
    }

    @Override
    public void deleteBucketLifecycle(String bucketName)
            throws OSSException, ClientException {
        this.deleteBucketLifecycle(new GenericRequest(bucketName));
    }

    @Override
    public void deleteBucketLifecycle(GenericRequest genericRequest)
            throws OSSException, ClientException {
        bucketOperation.deleteBucketLifecycle(genericRequest);
    }
    
    @Override
    public void setBucketTagging(String bucketName, Map<String, String> tags)
            throws OSSException, ClientException {
        this.setBucketTagging(new SetBucketTaggingRequest(bucketName, tags));
    }
    

    @Override
    public void setBucketTagging(String bucketName, TagSet tagSet)
            throws OSSException, ClientException {
        this.setBucketTagging(new SetBucketTaggingRequest(bucketName, tagSet));
    }

    @Override
    public void setBucketTagging(SetBucketTaggingRequest setBucketTaggingRequest)
            throws OSSException, ClientException {
        this.bucketOperation.setBucketTagging(setBucketTaggingRequest);
    }

    @Override
    public TagSet getBucketTagging(String bucketName) 
            throws OSSException, ClientException {
        return this.getBucketTagging(new GenericRequest(bucketName));
    }

    @Override
    public TagSet getBucketTagging(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketTagging(genericRequest);
    }

    @Override
    public void deleteBucketTagging(String bucketName) 
            throws OSSException, ClientException {
        this.deleteBucketTagging(new GenericRequest(bucketName));
    }

    @Override
    public void deleteBucketTagging(GenericRequest genericRequest)
            throws OSSException, ClientException {
        this.bucketOperation.deleteBucketTagging(genericRequest);
    }
    
    @Override
    public void addBucketReplication(AddBucketReplicationRequest addBucketReplicationRequest)
            throws OSSException, ClientException {
        this.bucketOperation.addBucketReplication(addBucketReplicationRequest);
    }

    @Override
    public List<ReplicationRule> getBucketReplication(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketReplication(new GenericRequest(bucketName));
    }

    @Override
    public List<ReplicationRule> getBucketReplication(GenericRequest genericRequest) 
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketReplication(genericRequest);
    }

    @Override
    public void deleteBucketReplication(String bucketName,
            String replicationRuleID) throws OSSException, ClientException {
        this.deleteBucketReplication(new DeleteBucketReplicationRequest(
                bucketName, replicationRuleID));
    }

    @Override
    public void deleteBucketReplication(
            DeleteBucketReplicationRequest deleteBucketReplicationRequest)
            throws OSSException, ClientException {
        this.bucketOperation.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @Override
    public BucketReplicationProgress getBucketReplicationProgress(
            String bucketName, String replicationRuleID) throws OSSException,
            ClientException {
        return this.getBucketReplicationProgress(new GetBucketReplicationProgressRequest(
                        bucketName, replicationRuleID));
    }

    @Override
    public BucketReplicationProgress getBucketReplicationProgress(
            GetBucketReplicationProgressRequest getBucketReplicationProgressRequest)
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketReplicationProgress(getBucketReplicationProgressRequest);
    }

    @Override
    public List<String> getBucketReplicationLocation(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketReplicationLocation(new GenericRequest(bucketName));
    }

    @Override
    public List<String> getBucketReplicationLocation(GenericRequest genericRequest) 
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketReplicationLocation(genericRequest);
    }

    @Override
    public void addBucketCname(AddBucketCnameRequest addBucketCnameRequest)
            throws OSSException, ClientException {
        this.bucketOperation.addBucketCname(addBucketCnameRequest);
    }

    @Override
    public List<CnameConfiguration> getBucketCname(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketCname(new GenericRequest(bucketName));
    }

    @Override
    public List<CnameConfiguration> getBucketCname(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketCname(genericRequest);
    }

    @Override
    public void deleteBucketCname(String bucketName, String domain)
            throws OSSException, ClientException {
        this.deleteBucketCname(new DeleteBucketCnameRequest(bucketName, domain));
    }

    @Override
    public void deleteBucketCname(DeleteBucketCnameRequest deleteBucketCnameRequest)
            throws OSSException, ClientException {
        this.bucketOperation.deleteBucketCname(deleteBucketCnameRequest);
    }
    
    @Override
    public BucketInfo getBucketInfo(String bucketName) throws OSSException,
            ClientException {
        return this.getBucketInfo(new GenericRequest(bucketName));
    }

    @Override
    public BucketInfo getBucketInfo(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketInfo(genericRequest);
    }
    
    @Override
    public BucketStat getBucketStat(String bucketName)
            throws OSSException, ClientException {
    	return this.getBucketStat(new GenericRequest(bucketName));
    }
    
    @Override
    public BucketStat getBucketStat(GenericRequest genericRequest)
            throws OSSException, ClientException {
    	return this.bucketOperation.getBucketStat(genericRequest);
    }
    
    @Override
    public void setBucketStorageCapacity(String bucketName, UserQos userQos) throws OSSException,
            ClientException {
        this.setBucketStorageCapacity(new SetBucketStorageCapacityRequest(bucketName).withUserQos(userQos)); 
    }

    @Override
    public void setBucketStorageCapacity(SetBucketStorageCapacityRequest setBucketStorageCapacityRequest)
            throws OSSException, ClientException {
        this.bucketOperation.setBucketStorageCapacity(setBucketStorageCapacityRequest);
    }

    @Override
    public UserQos getBucketStorageCapacity(String bucketName)
            throws OSSException, ClientException {
        return this.getBucketStorageCapacity(new GenericRequest(bucketName));
    }

    @Override
    public UserQos getBucketStorageCapacity(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return this.bucketOperation.getBucketStorageCapacity(genericRequest);
    }
	
	@Override
    public UploadFileResult uploadFile(UploadFileRequest uploadFileRequest) throws Throwable {
        return this.uploadOperation.uploadFile(uploadFileRequest);
    }
    
    @Override
    public DownloadFileResult downloadFile(DownloadFileRequest downloadFileRequest) throws Throwable {
        return downloadOperation.downloadFile(downloadFileRequest);
    }
    
    @Override
    public CreateLiveChannelResult createLiveChannel(CreateLiveChannelRequest createLiveChannelRequest) 
            throws OSSException, ClientException {
        return liveChannelOperation.createLiveChannel(createLiveChannelRequest);
    }
    
    @Override
    public void setLiveChannelStatus(String bucketName, String liveChannel, LiveChannelStatus status) 
            throws OSSException, ClientException {
        this.setLiveChannelStatus(new SetLiveChannelRequest(bucketName, liveChannel, status));
    }
    
    @Override
    public void setLiveChannelStatus(SetLiveChannelRequest setLiveChannelRequest) 
            throws OSSException, ClientException {
        liveChannelOperation.setLiveChannelStatus(setLiveChannelRequest);
    }
    
    @Override
    public LiveChannelInfo getLiveChannelInfo(String bucketName, String liveChannel) 
            throws OSSException, ClientException {
        return this.getLiveChannelInfo(new LiveChannelGenericRequest(bucketName, liveChannel));
    }
    
    @Override
    public LiveChannelInfo getLiveChannelInfo(LiveChannelGenericRequest liveChannelGenericRequest) 
            throws OSSException, ClientException {
        return liveChannelOperation.getLiveChannelInfo(liveChannelGenericRequest);
    }
    
    @Override
    public LiveChannelStat getLiveChannelStat(String bucketName, String liveChannel) 
            throws OSSException, ClientException {
        return this.getLiveChannelStat(new LiveChannelGenericRequest(bucketName, liveChannel));
    }
    
    @Override
    public LiveChannelStat getLiveChannelStat(LiveChannelGenericRequest liveChannelGenericRequest) 
            throws OSSException, ClientException {
        return liveChannelOperation.getLiveChannelStat(liveChannelGenericRequest);
    }
    
    @Override
    public void deleteLiveChannel(String bucketName, String liveChannel) 
            throws OSSException, ClientException {
        this.deleteLiveChannel(new LiveChannelGenericRequest(bucketName, liveChannel));
    }
    
    @Override
    public void deleteLiveChannel(LiveChannelGenericRequest liveChannelGenericRequest) 
            throws OSSException, ClientException {
        liveChannelOperation.deleteLiveChannel(liveChannelGenericRequest);
    }
    
    @Override
    public List<LiveChannel> listLiveChannels(String bucketName) throws OSSException, ClientException {
        return liveChannelOperation.listLiveChannels(bucketName);
    }
    
    @Override
    public LiveChannelListing listLiveChannels(ListLiveChannelsRequest listLiveChannelRequest) 
            throws OSSException, ClientException {
        return liveChannelOperation.listLiveChannels(listLiveChannelRequest);
    }
    
    @Override
    public List<LiveRecord> getLiveChannelHistory(String bucketName, String liveChannel) 
            throws OSSException, ClientException {
        return this.getLiveChannelHistory(new LiveChannelGenericRequest(bucketName, liveChannel));
    }
    
    @Override
    public List<LiveRecord> getLiveChannelHistory(LiveChannelGenericRequest liveChannelGenericRequest) 
            throws OSSException, ClientException {
        return liveChannelOperation.getLiveChannelHistory(liveChannelGenericRequest);
    }
    
    @Override
    public void generateVodPlaylist(String bucketName, String liveChannelName, String PlaylistName,
            long startTime, long endTime) throws OSSException, ClientException {
        this.generateVodPlaylist(new GenerateVodPlaylistRequest(bucketName, liveChannelName,
                PlaylistName, startTime, endTime));
    }
    
    @Override
    public void generateVodPlaylist(GenerateVodPlaylistRequest generateVodPlaylistRequest) 
            throws OSSException, ClientException {
        liveChannelOperation.generateVodPlaylist(generateVodPlaylistRequest);
    }
   
    @Override
    public String generateRtmpUri(String bucketName, String liveChannelName, String PlaylistName,
            long expires) throws OSSException, ClientException {
        return this.generateRtmpUri(new GenerateRtmpUriRequest(bucketName, liveChannelName,
                PlaylistName, expires));
    }
    
    @Override
    public String generateRtmpUri(GenerateRtmpUriRequest generateRtmpUriRequest) 
            throws OSSException, ClientException {
        return liveChannelOperation.generateRtmpUri(generateRtmpUriRequest);
    }
    
    @Override
    public void createSymlink(String bucketName, String symLink, String targetObject)
            throws OSSException, ClientException {
        this.createSymlink(new CreateSymlinkRequest(
                bucketName, symLink, targetObject));
    }

    @Override
    public void createSymlink(CreateSymlinkRequest createSymlinkRequest)
            throws OSSException, ClientException {
        objectOperation.createSymlink(createSymlinkRequest);
    }

    @Override
    public OSSSymlink getSymlink(String bucketName, String symLink)
            throws OSSException, ClientException {
        return this.getSymlink(new GenericRequest(bucketName, symLink));
    }
    
    @Override
    public OSSSymlink getSymlink(GenericRequest genericRequest)
            throws OSSException, ClientException {
        return objectOperation.getSymlink(genericRequest);
    }
    
    @Override
    public GenericResult processObject(ProcessObjectRequest processObjectRequest)
            throws OSSException, ClientException {
        return this.objectOperation.processObject(processObjectRequest);
    }
    
    @Override
    public void createUdf(CreateUdfRequest createUdfRequest)
            throws OSSException, ClientException {
    	this.udfOperation.createUdf(createUdfRequest);
	}
    
    @Override
    public UdfInfo getUdfInfo(UdfGenericRequest genericRequest) 
    		throws OSSException, ClientException {
    	return this.udfOperation.getUdfInfo(genericRequest);
    }
    
    @Override
    public List<UdfInfo> listUdfs() throws OSSException, ClientException {
    	return this.udfOperation.listUdfs();
    }
    
    @Override
    public void deleteUdf(UdfGenericRequest genericRequest) 
    		throws OSSException, ClientException {
    	this.udfOperation.deleteUdf(genericRequest);
    }
    
    @Override
    public void uploadUdfImage(UploadUdfImageRequest uploadUdfImageRequest) 
    		throws OSSException, ClientException {
    	this.udfOperation.uploadUdfImage(uploadUdfImageRequest);
    }
    
    @Override
    public List<UdfImageInfo> getUdfImageInfo(UdfGenericRequest genericRequest) 
    		throws OSSException, ClientException {
    	return this.udfOperation.getUdfImageInfo(genericRequest);
    }
    
    @Override
    public void deleteUdfImage(UdfGenericRequest genericRequest) 
    		throws OSSException, ClientException {
    	this.udfOperation.deleteUdfImage(genericRequest);
    }
    
    @Override
    public void createUdfApplication(CreateUdfApplicationRequest createUdfApplicationRequest) 
            throws OSSException, ClientException {
        this.udfOperation.createUdfApplication(createUdfApplicationRequest);
    }
    
    @Override
    public UdfApplicationInfo getUdfApplicationInfo(UdfGenericRequest genericRequest) 
            throws OSSException, ClientException {
        return this.udfOperation.getUdfApplicationInfo(genericRequest);
    }
    
    @Override
    public List<UdfApplicationInfo> listUdfApplications() throws OSSException, ClientException {
        return this.udfOperation.listUdfApplication();
    }
    
    @Override
    public void deleteUdfApplication(UdfGenericRequest genericRequest) 
            throws OSSException, ClientException {
        this.udfOperation.deleteUdfApplication(genericRequest);
    }
    
    @Override
    public void upgradeUdfApplication(UpgradeUdfApplicationRequest upgradeUdfApplicationRequest) 
            throws OSSException, ClientException {
        this.udfOperation.upgradeUdfApplication(upgradeUdfApplicationRequest);
    }
    
    @Override
    public void resizeUdfApplication(ResizeUdfApplicationRequest resizeUdfApplicationRequest) 
            throws OSSException, ClientException {
        this.udfOperation.resizeUdfApplication(resizeUdfApplicationRequest);
    }
    
    @Override
    public UdfApplicationLog getUdfApplicationLog(GetUdfApplicationLogRequest getUdfApplicationLogRequest) 
            throws OSSException, ClientException {
        return this.udfOperation.getUdfApplicationLog(getUdfApplicationLogRequest);
    }
    
    @Override
    public void shutdown() {
        try {
            serviceClient.shutdown();
        } catch(Exception e) {
            logException("shutdown throw exception: ", e);
        }
    }
    
}