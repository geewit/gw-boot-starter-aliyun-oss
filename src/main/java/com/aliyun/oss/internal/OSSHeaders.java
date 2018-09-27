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

import com.aliyun.oss.common.utils.HttpHeaders;

public interface OSSHeaders extends HttpHeaders {
    
    String OSS_PREFIX = "x-oss-";
    String OSS_USER_METADATA_PREFIX = "x-oss-meta-";

    String OSS_CANNED_ACL = "x-oss-acl";

    String OSS_SERVER_SIDE_ENCRYPTION = "x-oss-server-side-encryption";

    String GET_OBJECT_IF_MODIFIED_SINCE = "If-Modified-Since";
    String GET_OBJECT_IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
    String GET_OBJECT_IF_MATCH = "If-Match";
    String GET_OBJECT_IF_NONE_MATCH = "If-None-Match";
    
    String HEAD_OBJECT_IF_MODIFIED_SINCE = "If-Modified-Since";
    String HEAD_OBJECT_IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
    String HEAD_OBJECT_IF_MATCH = "If-Match";
    String HEAD_OBJECT_IF_NONE_MATCH = "If-None-Match";

    String COPY_OBJECT_SOURCE = "x-oss-copy-source";
    String COPY_SOURCE_RANGE = "x-oss-copy-source-range";
    String COPY_OBJECT_SOURCE_IF_MATCH = "x-oss-copy-source-if-match";
    String COPY_OBJECT_SOURCE_IF_NONE_MATCH = "x-oss-copy-source-if-none-match";
    String COPY_OBJECT_SOURCE_IF_UNMODIFIED_SINCE = "x-oss-copy-source-if-unmodified-since";
    String COPY_OBJECT_SOURCE_IF_MODIFIED_SINCE = "x-oss-copy-source-if-modified-since";
    String COPY_OBJECT_METADATA_DIRECTIVE = "x-oss-metadata-directive";
    
    String OSS_HEADER_REQUEST_ID = "x-oss-request-id";
    
    String ORIGIN = "origin";
    String ACCESS_CONTROL_REQUEST_METHOD = "Access-Control-Request-Method";
    String ACCESS_CONTROL_REQUEST_HEADER = "Access-Control-Request-Headers";
    
    String OSS_SECURITY_TOKEN = "x-oss-security-token";
    
    String OSS_NEXT_APPEND_POSITION = "x-oss-next-append-position";
    String OSS_HASH_CRC64_ECMA = "x-oss-hash-crc64ecma";
    String OSS_OBJECT_TYPE = "x-oss-object-type";
    
    String OSS_OBJECT_ACL = "x-oss-object-acl";
    
    String OSS_HEADER_CALLBACK = "x-oss-callback";
    String OSS_HEADER_CALLBACK_VAR = "x-oss-callback-var";
    
    String OSS_HEADER_SYMLINK_TARGET = "x-oss-symlink-target";
    
    String OSS_STORAGE_CLASS = "x-oss-storage-class";
    String OSS_RESTORE = "x-oss-restore";
    String OSS_ONGOING_RESTORE = "ongoing-request=\"true\"";
}
