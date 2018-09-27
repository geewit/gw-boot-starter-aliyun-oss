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

/**
 * OSS定义的错误代码。
 */
public interface OSSErrorCode {

    /**
     * 拒绝访问。
     */
    String ACCESS_DENIED = "AccessDenied";
    
    /**
     * 禁止访问。
     */
    String ACCESS_FORBIDDEN = "AccessForbidden";
    
    /**
     * Bucket 已存在 。
     */
    String BUCKET_ALREADY_EXISTS = "BucketAlreadyExists";
    
    /**
     * Bucket 不为空。
     */
    String BUCKET_NOT_EMPTY = "BucketNotEmpty";

    /**
     * 文件组过大。
     */
    String FILE_GROUP_TOO_LARGE = "FileGroupTooLarge";

    /**
     * 文件Part过时。
     */
    String FILE_PART_STALE = "FilePartStale";

    /**
     * 参数格式错误。
     */
    String INVALID_ARGUMENT = "InvalidArgument";

    /**
     * Access ID不存在。
     */
    String INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId";

    /**
     * 无效的 Bucket 名字。
     */
    String INVALID_BUCKET_NAME = "InvalidBucketName";

    /**
     * 无效的 Object 名字 。
     */
    String INVALID_OBJECT_NAME = "InvalidObjectName";

    /**
     * 无效的 Part。
     */
    String INVALID_PART = "InvalidPart";

    /**
     * 无效的 Part顺序。
     */
    String INVALID_PART_ORDER = "InvalidPartOrder";

    /**
     * 设置Bucket Logging时目标Bucket不存在。
     */
    String INVALID_TARGET_BUCKET_FOR_LOGGING = "InvalidTargetBucketForLogging";

    /**
     * OSS 内部发生错误。
     */
    String INTERNAL_ERROR = "InternalError";
    
    /**
     * 缺少内容长度。
     */
    String MISSING_CONTENT_LENGTH = "MissingContentLength";
    
    /**
     * 缺少必须参数。
     */
    String MISSING_ARGUMENT = "MissingArgument";

    /**
     * Bucket 不存在。
     */
    String NO_SUCH_BUCKET = "NoSuchBucket";

    /**
     * 文件不存在。
     */
    String NO_SUCH_KEY = "NoSuchKey";

    /**
     * 无法处理的方法。
     */
    String NOT_IMPLEMENTED = "NotImplemented";

    /**
     * 预处理错误。
     */
    String PRECONDITION_FAILED = "PreconditionFailed";

    /**
     * 304 Not Modified。
     */
    String NOT_MODIFIED = "NotModified";
    
    /**
     * 指定的数据中心非法。
     */
    String INVALID_LOCATION_CONSTRAINT = "InvalidLocationConstraint";
    
    /**
     * 指定的数据中心与请求的终端域名不一致。
     */
    String ILLEGAL_LOCATION_CONSTRAINT_EXCEPTION = "IllegalLocationConstraintException";
    
    /**
     * 发起请求的时间和服务器时间超出15分钟。
     */
    String REQUEST_TIME_TOO_SKEWED = "RequestTimeTooSkewed";

    /**
     * 请求超时。
     */
    String REQUEST_TIMEOUT = "RequestTimeout";

    /**
     * 签名错误。
     */
    String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";

    /**
     * 用户的 Bucket 数目超过限制 。
     */
    String TOO_MANY_BUCKETS = "TooManyBuckets";
    
    /**
     * 源Bucket未设置CORS
     */
    String NO_SUCH_CORS_CONFIGURATION="NoSuchCORSConfiguration";
    
    /**
     * 源Bucket未设置静态网站托管功能
     */
    String NO_SUCH_WEBSITE_CONFIGURATION="NoSuchWebsiteConfiguration";
    
    /**
     * 源Bucket未设置Lifecycle
     */
    String NO_SUCH_LIFECYCLE = "NoSuchLifecycle";
    
    /**
     * XML格式非法。
     */
    String MALFORMED_XML = "MalformedXML";
    
    /**
     * 无效的服务器端加密编码。
     */
    String INVALID_ENCRYPTION_ALGORITHM_ERROR = "InvalidEncryptionAlgorithmError";
    
    /**
     * Multipart Upload ID 不存在。
     */
    String NO_SUCH_UPLOAD = "NoSuchUpload";
    
    /**
     * 实体过小。
     */
    String ENTITY_TOO_SMALL = "EntityTooSmall";
    
    /**
     * 实体过大。
     */
    String ENTITY_TOO_LARGE = "EntityTooLarge";
    
    /**
     * 无效的MD5值。
     */
    String INVALID_DIGEST = "InvalidDigest";
    
    /**
     * 无效的字节范围。
     */
    String INVALID_RANGE = "InvalidRange";
    
    /**
     * 不支持安全令牌。
     */
    String SECURITY_TOKEN_NOT_SUPPORTED = "SecurityTokenNotSupported";
    
    /**
     * Object不支持追加。
     */
    String OBJECT_NOT_APPENDALBE = "ObjectNotAppendable";
    
    /**
     * Object追加位置与其当前长度不一致。
     */
    String POSITION_NOT_EQUAL_TO_LENGTH = "PositionNotEqualToLength";

    /**
     * 返回结果无法解析。
     */
    String INVALID_RESPONSE = "InvalidResponse";
    
    /**
     * 回调失败，表示OSS没有收到预期的回调响应，不代表应用服务器没有收到回调请求，此时文件已经成功上传到了OSS。
     */
    String CALLBACK_FAILED = "CallbackFailed";
    
    /**
     * Live channel不存在。
     */
    String NO_SUCH_LIVE_CHANNEL = "NoSuchLiveChannel";
    
    /**
     * 连接的目标文件不存在
     */
    String NO_SUCH_SYM_LINK_TARGET = "SymlinkTargetNotExist";
    
    /**
     * 冷文件没有预热直接使用。
     */
    String INVALID_OBJECT_STATE = "InvalidObjectState";
}
