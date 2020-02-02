package io.geewit.boot.aliyun.oss;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;
import io.geewit.boot.aliyun.AliyunProperties;
import io.geewit.boot.aliyun.oss.impl.FileStorageServiceOssImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;

/**
 * aliyun oss auto configuration
 *
 * @author linux_china
 */
@Configuration
@ConditionalOnClass(OSSClient.class)
@EnableConfigurationProperties({AliyunProperties.class, AliyunOssProperties.class})
public class AliyunOssAutoConfiguration {
    private final static Logger logger = LoggerFactory.getLogger(AliyunOssAutoConfiguration.class);

    private final Environment environment;

    private final AliyunProperties properties;

    private final AliyunOssProperties ossProperties;

    public AliyunOssAutoConfiguration(Environment environment, AliyunProperties properties, AliyunOssProperties ossProperties) {
        this.environment = environment;
        this.properties = properties;
        this.ossProperties = ossProperties;
}

    @Bean
    @ConditionalOnMissingBean
    public OSSClient ossClient() {
        ClientConfiguration config = new ClientConfiguration();
        if(ossProperties.getTimeout() != null && ossProperties.getTimeout() > 0) {
            config.setConnectionTimeout(ossProperties.getTimeout());
            config.setSocketTimeout(ossProperties.getTimeout());
            config.setRequestTimeout(ossProperties.getTimeout());
            config.setConnectionRequestTimeout(ossProperties.getTimeout());
        }

        return new OSSClient(ossProperties.getEndpoint(), properties.getKey(), properties.getSecret(), config);
    }

    @Bean
    @ConditionalOnMissingBean
    public FileStorageService fileStorageService() {
        OSSClient ossClient = ossClient();
        String bucketName = environment.getActiveProfiles()[0] + "-" + ossProperties.getBucket();
        boolean exists = ossClient.doesBucketExist(bucketName);
        if(exists) {
            logger.debug("bucket: [" + bucketName + "]已经存在");
            AccessControlList accessControlList = ossClient.getBucketAcl(bucketName);
            CannedAccessControlList existsCannedACL = accessControlList.getCannedACL();
            if(CannedAccessControlList.PublicRead.equals(existsCannedACL)) {
                logger.debug("权限已经为 PublicRead");
            } else {
                logger.debug("设置权限为 PublicRead");
                ossClient.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);
            }
            ObjectMetadata metadata = new ObjectMetadata();
            ByteArrayInputStream bis = new ByteArrayInputStream(new byte[0]);
            try {
                metadata.setContentType(MediaType.TEXT_PLAIN_VALUE);
                metadata.setContentLength(0);
                metadata.setCacheControl("public, max-age=31536000");
                ossClient.putObject(bucketName, AliyunOssProperties.DEFAULT_TEST_FILENAME, bis, metadata);
            } catch (Exception ignore) {
                ossClient.putObject(bucketName, AliyunOssProperties.DEFAULT_TEST_FILENAME, bis, metadata);
            }
        } else {
            logger.debug("bucket: [" + bucketName + "]不存在, 创建并设置权限 PublicRead");
            CreateBucketRequest createBucketRequest= new CreateBucketRequest(bucketName);
            // 设置bucket权限为公共读，默认是私有读写
            createBucketRequest.setCannedACL(CannedAccessControlList.PublicRead);
            // 设置bucket存储类型为低频访问类型，默认是标准类型
            createBucketRequest.setStorageClass(StorageClass.IA);
            ossClient.createBucket(createBucketRequest);
        }
        FileStorageServiceOssImpl fileStorageServiceOss = new FileStorageServiceOssImpl(ossClient);
        fileStorageServiceOss.setBucketName(bucketName);
        return fileStorageServiceOss;
    }
}
