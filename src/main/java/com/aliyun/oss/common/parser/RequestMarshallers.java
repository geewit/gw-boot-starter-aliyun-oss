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

package com.aliyun.oss.common.parser;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.common.comm.io.FixedLengthInputStream;
import com.aliyun.oss.common.utils.DateUtil;
import com.aliyun.oss.internal.RequestParameters;
import com.aliyun.oss.model.*;
import com.aliyun.oss.model.AddBucketReplicationRequest.ReplicationAction;
import com.aliyun.oss.model.LifecycleRule.AbortMultipartUpload;
import com.aliyun.oss.model.LifecycleRule.RuleStatus;
import com.aliyun.oss.model.LifecycleRule.StorageTransition;
import com.aliyun.oss.model.SetBucketCORSRequest.CORSRule;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import static com.aliyun.oss.internal.OSSConstants.DEFAULT_CHARSET_NAME;

/**
 * A collection of marshallers that marshall HTTP request into crossponding input stream. 
 */
public final class RequestMarshallers {
    
    static final StringMarshaller stringMarshaller = new StringMarshaller();
    
    public static final DeleteObjectsRequestMarshaller deleteObjectsRequestMarshaller = new DeleteObjectsRequestMarshaller();
    
    public static final CreateBucketRequestMarshaller createBucketRequestMarshaller = new CreateBucketRequestMarshaller();
    public static final BucketRefererMarshaller bucketRefererMarshaller = new BucketRefererMarshaller();
    public static final SetBucketLoggingRequestMarshaller setBucketLoggingRequestMarshaller = new SetBucketLoggingRequestMarshaller();
    public static final SetBucketWebsiteRequestMarshaller setBucketWebsiteRequestMarshaller = new SetBucketWebsiteRequestMarshaller();
    public static final SetBucketLifecycleRequestMarshaller setBucketLifecycleRequestMarshaller = new SetBucketLifecycleRequestMarshaller();
	public static final PutBucketImageRequestMarshaller putBucketImageRequestMarshaller = new PutBucketImageRequestMarshaller();
	public static final PutImageStyleRequestMarshaller putImageStyleRequestMarshaller = new PutImageStyleRequestMarshaller();
	public static final BucketImageProcessConfMarshaller bucketImageProcessConfMarshaller = new BucketImageProcessConfMarshaller();
	public static final SetBucketCORSRequestMarshaller setBucketCORSRequestMarshaller = new SetBucketCORSRequestMarshaller();
    public static final SetBucketTaggingRequestMarshaller setBucketTaggingRequestMarshaller = new SetBucketTaggingRequestMarshaller();
    public static final AddBucketReplicationRequestMarshaller addBucketReplicationRequestMarshaller = new AddBucketReplicationRequestMarshaller();
    public static final DeleteBucketReplicationRequestMarshaller deleteBucketReplicationRequestMarshaller = new DeleteBucketReplicationRequestMarshaller();    
    public static final AddBucketCnameRequestMarshaller addBucketCnameRequestMarshaller = new AddBucketCnameRequestMarshaller();
    public static final DeleteBucketCnameRequestMarshaller deleteBucketCnameRequestMarshaller = new DeleteBucketCnameRequestMarshaller();    
    public static final SetBucketQosRequestMarshaller setBucketQosRequestMarshaller = new SetBucketQosRequestMarshaller();    
    public static final CompleteMultipartUploadRequestMarshaller completeMultipartUploadRequestMarshaller = new CompleteMultipartUploadRequestMarshaller();
    public static final CreateLiveChannelRequestMarshaller createLiveChannelRequestMarshaller = new CreateLiveChannelRequestMarshaller();
    public static final CreateUdfRequestMarshaller createUdfRequestMarshaller = new CreateUdfRequestMarshaller();
    public static final CreateUdfApplicationRequestMarshaller createUdfApplicationRequestMarshaller = new CreateUdfApplicationRequestMarshaller();
    public static final UpgradeUdfApplicationRequestMarshaller upgradeUdfApplicationRequestMarshaller = new UpgradeUdfApplicationRequestMarshaller();
    public static final ResizeUdfApplicationRequestMarshaller resizeUdfApplicationRequestMarshaller = new ResizeUdfApplicationRequestMarshaller();
    public static final ProcessObjectRequestMarshaller processObjectRequestMarshaller = new ProcessObjectRequestMarshaller();

    interface RequestMarshaller<R> extends Marshaller<FixedLengthInputStream, R> {
        
    }
    
    interface RequestMarshaller2<R> extends Marshaller<byte[], R> {
        
    }
    
    public static final class StringMarshaller implements Marshaller<FixedLengthInputStream, String> {

        @Override
        public FixedLengthInputStream marshall(String input) {
            if (input == null) {
                throw new IllegalArgumentException("The input should not be null.");
            }
            
            byte[] binaryData;
            try {
                binaryData = input.getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            long length = binaryData.length;
            InputStream instream = new ByteArrayInputStream(binaryData);
            return new FixedLengthInputStream(instream, length);
        }
        
    }
    
	public static final class PutImageStyleRequestMarshaller implements RequestMarshaller<PutImageStyleRequest> {
		@Override
		public FixedLengthInputStream marshall(PutImageStyleRequest request) {
            String xmlBody = "<Style>" +
                    "<Content>" + request.GetStyle() + "</Content>" +
                    "</Style>";
            return stringMarshaller.marshall(xmlBody);
		}
	}
	
    public static final class BucketImageProcessConfMarshaller implements RequestMarshaller<ImageProcess> {

        @Override
        public FixedLengthInputStream marshall(ImageProcess imageProcessConf) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<BucketProcessConfiguration>");
            xmlBody.append("<CompliedHost>" + imageProcessConf.getCompliedHost() + "</CompliedHost>");
            if (imageProcessConf.isSourceFileProtect() != null &&
                    imageProcessConf.isSourceFileProtect()) {
                xmlBody.append("<SourceFileProtect>Enabled</SourceFileProtect>");
            } else {
                xmlBody.append("<SourceFileProtect>Disabled</SourceFileProtect>");
            }
            xmlBody.append("<SourceFileProtectSuffix>").append(imageProcessConf.getSourceFileProtectSuffix()).append("</SourceFileProtectSuffix>");
            xmlBody.append("<StyleDelimiters>" + imageProcessConf.getStyleDelimiters() + "</StyleDelimiters>");
            if (imageProcessConf.isSupportAtStyle() != null &&
                    imageProcessConf.isSupportAtStyle()) {
                xmlBody.append("<OssDomainSupportAtProcess>Enabled</OssDomainSupportAtProcess>");
            } else {
                xmlBody.append("<OssDomainSupportAtProcess>Disabled</OssDomainSupportAtProcess>");
            }
            xmlBody.append("</BucketProcessConfiguration>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }

	public static final class PutBucketImageRequestMarshaller  implements RequestMarshaller<PutBucketImageRequest> {
		@Override
		public FixedLengthInputStream marshall(PutBucketImageRequest request) {
			StringBuilder xmlBody = new StringBuilder();
	        xmlBody.append("<Channel>");
	        if (request.GetIsForbidOrigPicAccess()) {
	        	xmlBody.append("<OrigPicForbidden>true</OrigPicForbidden>");
	        } else {
	        	xmlBody.append("<OrigPicForbidden>false</OrigPicForbidden>");
	        }
	        
	        if (request.GetIsUseStyleOnly()) {
	        	xmlBody.append("<UseStyleOnly>true</UseStyleOnly>");
	        } else {
	        	xmlBody.append("<UseStyleOnly>false</UseStyleOnly>");
	        }
	        
	        if (request.GetIsAutoSetContentType()) {
	        	xmlBody.append("<AutoSetContentType>true</AutoSetContentType>");
	        } else {
	        	xmlBody.append("<AutoSetContentType>false</AutoSetContentType>");
	        }
	        
	        if (request.GetIsUseSrcFormat()) {
	        	xmlBody.append("<UseSrcFormat>true</UseSrcFormat>");
	        } else {
	        	xmlBody.append("<UseSrcFormat>false</UseSrcFormat>");
	        }
	        
	        if (request.GetIsSetAttachName()) {
	        	xmlBody.append("<SetAttachName>true</SetAttachName>");
	        } else {
	        	xmlBody.append("<SetAttachName>false</SetAttachName>");
	        }
	        xmlBody.append("<Default404Pic>").append(request.GetDefault404Pic()).append("</Default404Pic>");
	        xmlBody.append("<StyleDelimiters>").append(request.GetStyleDelimiters()).append("</StyleDelimiters>");
	        
	        xmlBody.append("</Channel>");
			return stringMarshaller.marshall(xmlBody.toString());
		}
	}
    
    public static final class CreateBucketRequestMarshaller implements RequestMarshaller<CreateBucketRequest> {

        @Override
        public FixedLengthInputStream marshall(CreateBucketRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            if (request.getLocationConstraint() != null || request.getStorageClass() != null) {
                xmlBody.append("<CreateBucketConfiguration>");
                if (request.getLocationConstraint() != null) {
                    xmlBody.append("<LocationConstraint>").append(request.getLocationConstraint()).append("</LocationConstraint>");
                }
                if (request.getStorageClass() != null) {
                    xmlBody.append("<StorageClass>").append(request.getStorageClass().toString()).append("</StorageClass>");
                }
                xmlBody.append("</CreateBucketConfiguration>");
            }
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class BucketRefererMarshaller implements RequestMarshaller<BucketReferer> {

        @Override
        public FixedLengthInputStream marshall(BucketReferer br) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<RefererConfiguration>");
            xmlBody.append("<AllowEmptyReferer>").append(br.isAllowEmptyReferer()).append("</AllowEmptyReferer>");
            
            if (!br.getRefererList().isEmpty()) {
                xmlBody.append("<RefererList>");
                for (String referer : br.getRefererList()) {
                    xmlBody.append("<Referer>").append(referer).append("</Referer>");
                }
                xmlBody.append("</RefererList>");
            } else {
                xmlBody.append("<RefererList/>");
            }
            
            xmlBody.append("</RefererConfiguration>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class SetBucketLoggingRequestMarshaller implements RequestMarshaller<SetBucketLoggingRequest> {

        @Override
        public FixedLengthInputStream marshall(SetBucketLoggingRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<BucketLoggingStatus>");
            if (request.getTargetBucket() != null) {
                xmlBody.append("<LoggingEnabled>");
                xmlBody.append("<TargetBucket>" + request.getTargetBucket() + "</TargetBucket>");
                if(request.getTargetPrefix() != null) {
                    xmlBody.append("<TargetPrefix>" + request.getTargetPrefix() + "</TargetPrefix>");
                }
                xmlBody.append("</LoggingEnabled>");
            }
            xmlBody.append("</BucketLoggingStatus>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }

    public static final class SetBucketWebsiteRequestMarshaller implements RequestMarshaller<SetBucketWebsiteRequest> {

        @Override
        public FixedLengthInputStream marshall(SetBucketWebsiteRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<WebsiteConfiguration>");
            if(request.getIndexDocument() != null){
                xmlBody.append("<IndexDocument>");
                xmlBody.append("<Suffix>").append(request.getIndexDocument()).append("</Suffix>");
                xmlBody.append("</IndexDocument>");
            }
            if(request.getErrorDocument() != null){
                xmlBody.append("<ErrorDocument>");
                xmlBody.append("<Key>").append(request.getErrorDocument()).append("</Key>");
                xmlBody.append("</ErrorDocument>");
            }
            
            // RoutingRules可以没有
            if (request.getRoutingRules().size() > 0) {
                xmlBody.append("<RoutingRules>");
                for (RoutingRule routingRule : request.getRoutingRules()) {
                    xmlBody.append("<RoutingRule>");
                    xmlBody.append("<RuleNumber>").append(routingRule.getNumber()).append("</RuleNumber>");
                    
                    // Condition字句可以没有，如果有至少有一个条件
                    RoutingRule.Condition condition = routingRule.getCondition();
                    if (condition.getKeyPrefixEquals() != null || condition.getHttpErrorCodeReturnedEquals() > 0) {
                        xmlBody.append("<Condition>");
                        if (condition.getKeyPrefixEquals() != null) {
                            xmlBody.append("<KeyPrefixEquals>").append(escapeKey(condition.getKeyPrefixEquals())).append("</KeyPrefixEquals>");
                        }
                        if (condition.getHttpErrorCodeReturnedEquals() != null) {
                            xmlBody.append("<HttpErrorCodeReturnedEquals>").append(condition.getHttpErrorCodeReturnedEquals()).append("</HttpErrorCodeReturnedEquals>");
                        }
                        xmlBody.append("</Condition>");
                    }
                    
                    // Redirect子句必须存在
                    RoutingRule.Redirect redirect = routingRule.getRedirect();
                    xmlBody.append("<Redirect>");
                    if (redirect.getRedirectType() != null) {
                        xmlBody.append("<RedirectType>").append(redirect.getRedirectType().toString()).append("</RedirectType>");
                    }
                    if (redirect.getHostName() != null) {
                        xmlBody.append("<HostName>").append(redirect.getHostName()).append("</HostName>");
                    }
                    if (redirect.getProtocol() != null) {
                        xmlBody.append("<Protocol>").append(redirect.getProtocol().toString()).append("</Protocol>");
                    }
                    if (redirect.getReplaceKeyPrefixWith() != null) {
                        xmlBody.append("<ReplaceKeyPrefixWith>").append(escapeKey(redirect.getReplaceKeyPrefixWith())).append("</ReplaceKeyPrefixWith>");
                    }
                    if (redirect.getReplaceKeyWith() != null) {
                        xmlBody.append("<ReplaceKeyWith>").append(escapeKey(redirect.getReplaceKeyWith())).append("</ReplaceKeyWith>");
                    }
                    if (redirect.getHttpRedirectCode() != null) {
                        xmlBody.append("<HttpRedirectCode>").append(redirect.getHttpRedirectCode()).append("</HttpRedirectCode>");
                    }
                    if (redirect.getMirrorURL() != null) {
                        xmlBody.append("<MirrorURL>").append(redirect.getMirrorURL()).append("</MirrorURL>");
                    }
                    if (redirect.getMirrorSecondaryURL() != null) {
                        xmlBody.append("<MirrorURLSlave>").append(redirect.getMirrorSecondaryURL()).append("</MirrorURLSlave>");
                    }
                    if (redirect.getMirrorProbeURL() != null) {
                        xmlBody.append("<MirrorURLProbe>").append(redirect.getMirrorProbeURL()).append("</MirrorURLProbe>");
                    }
                    if (redirect.isPassQueryString() != null) {
                        xmlBody.append("<MirrorPassQueryString>").append(redirect.isPassQueryString()).append("</MirrorPassQueryString>");
                    }
                    if (redirect.isPassOriginalSlashes() != null) {
                        xmlBody.append("<MirrorPassOriginalSlashes>").append(redirect.isPassOriginalSlashes()).append("</MirrorPassOriginalSlashes>");
                    }
                    xmlBody.append("</Redirect>");
                    xmlBody.append("</RoutingRule>");
                }
                xmlBody.append("</RoutingRules>");
            }
            
            xmlBody.append("</WebsiteConfiguration>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class SetBucketLifecycleRequestMarshaller implements RequestMarshaller<SetBucketLifecycleRequest> {

        @Override
        public FixedLengthInputStream marshall(SetBucketLifecycleRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<LifecycleConfiguration>");
            for (LifecycleRule rule : request.getLifecycleRules()) {
                xmlBody.append("<Rule>");
                
                if (rule.getId() != null) {
                    xmlBody.append("<ID>").append(rule.getId()).append("</ID>");
                }
                
                if (rule.getPrefix() != null) {
                    xmlBody.append("<Prefix>").append(rule.getPrefix()).append("</Prefix>");
                } else {
                    xmlBody.append("<Prefix></Prefix>");
                }
                
                if (rule.getStatus() == RuleStatus.Enabled) {
                    xmlBody.append("<Status>Enabled</Status>");
                } else {
                    xmlBody.append("<Status>Disabled</Status>");
                }
                
                if (rule.getExpirationTime() != null) {
                    String formatDate = DateUtil.formatIso8601Date(rule.getExpirationTime());
                    xmlBody.append("<Expiration><Date>").append(formatDate).append("</Date></Expiration>");
                } else if (rule.getExpirationDays() != 0) {
                    xmlBody.append("<Expiration><Days>").append(rule.getExpirationDays()).append("</Days></Expiration>");
                } else if (rule.getCreatedBeforeDate() != null){
                    String formatDate = DateUtil.formatIso8601Date(rule.getCreatedBeforeDate());
                    xmlBody.append("<Expiration><CreatedBeforeDate>").append(formatDate).append("</CreatedBeforeDate></Expiration>");
                }
                
                if (rule.hasAbortMultipartUpload()) {
                    AbortMultipartUpload abortMultipartUpload = rule.getAbortMultipartUpload();
                    if (abortMultipartUpload.getExpirationDays() != 0) {
                        xmlBody.append("<AbortMultipartUpload><Days>").append(abortMultipartUpload.getExpirationDays()).append("</Days></AbortMultipartUpload>");
                    } else {
                        String formatDate = DateUtil.formatIso8601Date(abortMultipartUpload.getCreatedBeforeDate());
                        xmlBody.append("<AbortMultipartUpload><CreatedBeforeDate>").append(formatDate).append("</CreatedBeforeDate></AbortMultipartUpload>");
                    }
                }
                
                if (rule.hasStorageTransition()) {
                    for (StorageTransition storageTransition : rule.getStorageTransition()) {
                        xmlBody.append("<Transition>");
                        if (storageTransition.hasExpirationDays()) {
                            xmlBody.append("<Days>").append(storageTransition.getExpirationDays()).append("</Days>");
                        } else if (storageTransition.hasCreatedBeforeDate()) {
                            String formatDate = DateUtil.formatIso8601Date(storageTransition.getCreatedBeforeDate());
                            xmlBody.append("<CreatedBeforeDate>").append(formatDate).append("</CreatedBeforeDate>");
                        }
                        xmlBody.append("<StorageClass>").append(storageTransition.getStorageClass()).append("</StorageClass>");
                        xmlBody.append("</Transition>");
                    }
                }
                
                xmlBody.append("</Rule>");
            }
            xmlBody.append("</LifecycleConfiguration>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class SetBucketCORSRequestMarshaller implements RequestMarshaller<SetBucketCORSRequest> {
        
        @Override
        public FixedLengthInputStream marshall(SetBucketCORSRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<CORSConfiguration>");
            for (CORSRule rule : request.getCorsRules()) {
                xmlBody.append("<CORSRule>");
                
                for (String allowedOrigin : rule.getAllowedOrigins()) {
                    xmlBody.append("<AllowedOrigin>").append(allowedOrigin).append("</AllowedOrigin>");
                }
                
                for (String allowedMethod : rule.getAllowedMethods()) {
                    xmlBody.append("<AllowedMethod>").append(allowedMethod).append("</AllowedMethod>");
                }
                
                if (rule.getAllowedHeaders().size() > 0) {
                    for(String allowedHeader : rule.getAllowedHeaders()){
                        xmlBody.append("<AllowedHeader>").append(allowedHeader).append("</AllowedHeader>");
                    }
                }
                
                if (rule.getExposeHeaders().size() > 0) {
                    for (String exposeHeader : rule.getExposeHeaders()) {
                        xmlBody.append("<ExposeHeader>").append(exposeHeader).append("</ExposeHeader>");
                    }
                }
                
                if(null != rule.getMaxAgeSeconds()) {
                    xmlBody.append("<MaxAgeSeconds>").append(rule.getMaxAgeSeconds()).append("</MaxAgeSeconds>");
                }
                
                xmlBody.append("</CORSRule>");
            }
            xmlBody.append("</CORSConfiguration>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class CompleteMultipartUploadRequestMarshaller implements RequestMarshaller<CompleteMultipartUploadRequest> {

        @Override
        public FixedLengthInputStream marshall(CompleteMultipartUploadRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            List<PartETag> eTags =  request.getPartETags();
            xmlBody.append("<CompleteMultipartUpload>");
            for (PartETag part : eTags) {
                String eTag = EscapedChar.QUOT + part.getETag().replace("\"", "") + EscapedChar.QUOT;
                xmlBody.append("<Part>");
                xmlBody.append("<PartNumber>").append(part.getPartNumber()).append("</PartNumber>");
                xmlBody.append("<ETag>").append(eTag).append("</ETag>");
                xmlBody.append("</Part>");
            }
            xmlBody.append("</CompleteMultipartUpload>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class DeleteObjectsRequestMarshaller implements RequestMarshaller2<DeleteObjectsRequest> {

        @Override
        public byte[] marshall(DeleteObjectsRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            boolean quiet = request.isQuiet();
            List<String> keysToDelete =  request.getKeys();
            
            xmlBody.append("<Delete>");
            xmlBody.append("<Quiet>").append(quiet).append("</Quiet>");
            for (String key : keysToDelete) {
                xmlBody.append("<Object>");
                xmlBody.append("<Key>").append(escapeKey(key)).append("</Key>");
                xmlBody.append("</Object>");
            }
            xmlBody.append("</Delete>");
            
            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class SetBucketTaggingRequestMarshaller implements RequestMarshaller<SetBucketTaggingRequest> {

        @Override
        public FixedLengthInputStream marshall(SetBucketTaggingRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            TagSet tagSet =  request.getTagSet();
            xmlBody.append("<Tagging><TagSet>");
            Map<String, String> tags = tagSet.getAllTags();
            if (!tags.isEmpty()) {
                for (Map.Entry<String, String> tag : tags.entrySet()) {
                    xmlBody.append("<Tag>");
                    xmlBody.append("<Key>").append(tag.getKey()).append("</Key>");
                    xmlBody.append("<Value>").append(tag.getValue()).append("</Value>");
                    xmlBody.append("</Tag>");
                }
            }
            xmlBody.append("</TagSet></Tagging>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class AddBucketReplicationRequestMarshaller implements RequestMarshaller<AddBucketReplicationRequest> {
        
        @Override
        public FixedLengthInputStream marshall(AddBucketReplicationRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<ReplicationConfiguration>");
            xmlBody.append("<Rule>");
            xmlBody.append("<ID>").append(escapeKey(request.getReplicationRuleID())).append("</ID>");
            xmlBody.append("<Destination>");
            xmlBody.append("<Bucket>").append(request.getTargetBucketName()).append("</Bucket>");
            xmlBody.append("<Location>").append(request.getTargetBucketLocation()).append("</Location>");
            xmlBody.append("</Destination>");
            xmlBody.append("<HistoricalObjectReplication>");
            if (request.isEnableHistoricalObjectReplication()) {
                xmlBody.append("enabled");
            } else {
                xmlBody.append("disabled");
            }
            xmlBody.append("</HistoricalObjectReplication>");
            if (request.getObjectPrefixList() != null && request.getObjectPrefixList().size() > 0) {
                xmlBody.append("<PrefixSet>");
                for (String prefix : request.getObjectPrefixList()) {
                    xmlBody.append("<Prefix>").append(prefix).append("</Prefix>");
                }
                xmlBody.append("</PrefixSet>");
            }
            if (request.getReplicationActionList() != null && request.getReplicationActionList().size() > 0) {
                xmlBody.append("<Action>").append(RequestMarshallers.joinRepliationAction(
                        request.getReplicationActionList())).append("</Action>");
            }
            xmlBody.append("</Rule>");
            xmlBody.append("</ReplicationConfiguration>");
            return stringMarshaller.marshall(xmlBody.toString());
        }
        
    }
    
    public static final class DeleteBucketReplicationRequestMarshaller implements RequestMarshaller2<DeleteBucketReplicationRequest> {

        @Override
        public byte[] marshall(DeleteBucketReplicationRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<ReplicationRules>");
            xmlBody.append("<ID>").append(escapeKey(request.getReplicationRuleID())).append("</ID>");
            xmlBody.append("</ReplicationRules>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class AddBucketCnameRequestMarshaller implements RequestMarshaller2<AddBucketCnameRequest> {

        @Override
        public byte[] marshall(AddBucketCnameRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<BucketCnameConfiguration>");
            xmlBody.append("<Cname>");
            xmlBody.append("<Domain>").append(request.getDomain()).append("</Domain>");
            xmlBody.append("</Cname>");
            xmlBody.append("</BucketCnameConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class DeleteBucketCnameRequestMarshaller implements RequestMarshaller2<DeleteBucketCnameRequest> {

        @Override
        public byte[] marshall(DeleteBucketCnameRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<BucketCnameConfiguration>");
            xmlBody.append("<Cname>");
            xmlBody.append("<Domain>").append(request.getDomain()).append("</Domain>");
            xmlBody.append("</Cname>");
            xmlBody.append("</BucketCnameConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class SetBucketQosRequestMarshaller implements RequestMarshaller2<UserQos> {

        @Override
        public byte[] marshall(UserQos userQos) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<BucketUserQos>");
            if (userQos.hasStorageCapacity()) {
                xmlBody.append("<StorageCapacity>").append(userQos.getStorageCapacity()).append("</StorageCapacity>");
            }
            xmlBody.append("</BucketUserQos>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class CreateLiveChannelRequestMarshaller implements RequestMarshaller2<CreateLiveChannelRequest> {

        @Override
        public byte[] marshall(CreateLiveChannelRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            xmlBody.append("<LiveChannelConfiguration>");
            xmlBody.append("<Description>").append(request.getLiveChannelDescription()).append("</Description>");
            xmlBody.append("<Status>").append(request.getLiveChannelStatus()).append("</Status>");
            
            LiveChannelTarget target = request.getLiveChannelTarget();
            xmlBody.append("<Target>");
            xmlBody.append("<Type>").append(target.getType()).append("</Type>");
            xmlBody.append("<FragDuration>").append(target.getFragDuration()).append("</FragDuration>");
            xmlBody.append("<FragCount>").append(target.getFragCount()).append("</FragCount>");
            xmlBody.append("<PlaylistName>").append(target.getPlaylistName()).append("</PlaylistName>");
            xmlBody.append("</Target>");
            xmlBody.append("</LiveChannelConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class CreateUdfRequestMarshaller implements RequestMarshaller2<CreateUdfRequest> {
    	
        @Override
        public byte[] marshall(CreateUdfRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            
            xmlBody.append("<CreateUDFConfiguration>");
            xmlBody.append("<Name>").append(request.getName()).append("</Name>");
            if (request.getId() != null) {
            	xmlBody.append("<ID>").append(request.getId()).append("</ID>");
            }
            if (request.getDesc() != null) {
            	xmlBody.append("<Description>").append(request.getDesc()).append("</Description>");
            }
            xmlBody.append("</CreateUDFConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class CreateUdfApplicationRequestMarshaller implements RequestMarshaller2<CreateUdfApplicationRequest> {
        
        @Override
        public byte[] marshall(CreateUdfApplicationRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            UdfApplicationConfiguration config = request.getUdfApplicationConfiguration();
            
            xmlBody.append("<CreateUDFApplicationConfiguration>");
            xmlBody.append("<ImageVersion>").append(config.getImageVersion()).append("</ImageVersion>");
            xmlBody.append("<InstanceNum>").append(config.getInstanceNum()).append("</InstanceNum>");
            xmlBody.append("<Flavor>");
            xmlBody.append("<InstanceType>").append(config.getFlavor().getInstanceType()).append("</InstanceType>");
            xmlBody.append("</Flavor>");
            xmlBody.append("</CreateUDFApplicationConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class UpgradeUdfApplicationRequestMarshaller implements RequestMarshaller2<UpgradeUdfApplicationRequest> {
        
        @Override
        public byte[] marshall(UpgradeUdfApplicationRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            
            xmlBody.append("<UpgradeUDFApplicationConfiguration>");
            xmlBody.append("<ImageVersion>").append(request.getImageVersion()).append("</ImageVersion>");
            xmlBody.append("</UpgradeUDFApplicationConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    
    public static final class ResizeUdfApplicationRequestMarshaller implements RequestMarshaller2<ResizeUdfApplicationRequest> {
        
        @Override
        public byte[] marshall(ResizeUdfApplicationRequest request) {
            StringBuilder xmlBody = new StringBuilder();
            
            xmlBody.append("<ResizeUDFApplicationConfiguration>");
            xmlBody.append("<InstanceNum>").append(request.getInstanceNum()).append("</InstanceNum>");
            xmlBody.append("</ResizeUDFApplicationConfiguration>");

            byte[] rawData;
            try {
                rawData = xmlBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    public static final class ProcessObjectRequestMarshaller implements RequestMarshaller2<ProcessObjectRequest> {
        
        @Override
        public byte[] marshall(ProcessObjectRequest request) {
            StringBuilder processBody = new StringBuilder();
            
            processBody.append(RequestParameters.SUBRESOURCE_PROCESS);
            processBody.append("=").append(request.getProcess());

            byte[] rawData;
            try {
                rawData = processBody.toString().getBytes(DEFAULT_CHARSET_NAME);
            } catch (UnsupportedEncodingException e) {
                throw new ClientException("Unsupported encoding " + e.getMessage(), e);
            }
            return rawData;
        }
        
    }
    
    private enum EscapedChar {
        // "\r"
        RETURN("&#x000D;"),
        
        // "\n"
        NEWLINE("&#x000A;"),
        
        // " "
        SPACE("&#x0020;"),
        
        // "\t"
        TAB("&#x0009;"),
        
        // """
        QUOT("&quot;"),
        
        // "&"
        AMP("&amp;"),
        
        // "<"
        LT("&lt;"),
        
        // ">"
        GT("&gt;");
        
        private final String escapedChar;
        
        EscapedChar(String escapedChar) {
            this.escapedChar = escapedChar;
        }
        
        @Override
        public String toString() {
            return this.escapedChar;
        }
    }
    
    private static String escapeKey(String key) {
        if (key == null) {
            return "";
        }
        
        int pos;
        int len = key.length();
        StringBuilder builder = new StringBuilder();
        for (pos = 0; pos < len; pos++) {
            char ch = key.charAt(pos);
            EscapedChar escapedChar;
            switch (ch) {
            case '\t':
                escapedChar = EscapedChar.TAB;
                break;
            case '\n':
                escapedChar = EscapedChar.NEWLINE;
                break;
            case '\r':
                escapedChar = EscapedChar.RETURN;
                break;
            case '&':
                escapedChar = EscapedChar.AMP;
                break;
            case '"':
                escapedChar = EscapedChar.QUOT;
                break;
            case '<':
                escapedChar = EscapedChar.LT;
                break;
            case '>':
                escapedChar = EscapedChar.GT;
                break;
            default:
                escapedChar = null;
                break;
            }

            if (escapedChar != null) {
                builder.append(escapedChar.toString());
            } else {
                builder.append(ch);
            }
        }
        
        return builder.toString();
    }
    
    private static String joinRepliationAction(List <ReplicationAction> actions) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        
        for (ReplicationAction action : actions) {
            if (!first) {
                sb.append(",");
            }
            sb.append(action);
            
            first = false;
        }

        return sb.toString();
    }
    
}
