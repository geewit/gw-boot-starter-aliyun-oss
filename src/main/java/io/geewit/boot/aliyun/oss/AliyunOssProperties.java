package io.geewit.boot.aliyun.oss;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * aliyun oss properties
 *
 * @author linux_china
 */
@ConfigurationProperties(prefix = "aliyun.oss")
public class AliyunOssProperties {
    protected final static String DEFAULT_TEST_FILENAME = "ok.txt";
    /**
     * default bucket
     */
    private String bucket;
    /**
     * endpoint
     */
    private String endpoint;

    /**
     * connection timeout
     */
    private Integer timeout;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }
}
