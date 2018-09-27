package io.geewit.boot.aliyun.oss;

import javax.activation.DataSource;
import java.io.IOException;

/**
 * file storage service
 *
 * @author linux_china
 */
public interface FileStorageService {

    String getBucketName();

    /**
     * save file
     *
     * @param ds data source
     * @return file name
     * @throws IOException IO Exception
     */
    String save(DataSource ds) throws IOException;

    /**
     * save file to the directory
     *
     * @param directory directory
     * @param ds        data source
     * @return file name with directory name
     * @throws IOException IO Exception
     */
    String saveToDirectory(String directory, DataSource ds) throws IOException;

    /**
     * delete file
     *
     * @param fileName file name
     * @throws IOException IO Exception
     */
    void delete(String fileName) throws IOException;

    /**
     * get file data source
     *
     * @param fileName file name
     * @return file data source
     * @throws IOException IO Exception
     */
    DataSource get(String fileName) throws IOException;

    void rename(String oldName, String newName) throws IOException;
}
