package io.geewit.boot.aliyun.oss;


import java.io.*;
import javax.activation.*;

/**
 * A DataSource backed by a byte array.  The byte array may be
 * passed in directly, or may be initialized from an InputStream
 * or a String.
 *
 * @author John Mani
 * @author Bill Shannon
 * @author Max Spivak
 * @since JavaMail 1.4
 */
public class ByteArrayDataSource implements DataSource {
    private byte[] data;    // data
    private int len = -1;
    private String type;    // content-type
    private String name = "";

    /**
     * Create a ByteArrayDataSource with data from the
     * specified byte array and with the specified MIME type.
     *
     * @param data the data
     * @param type the MIME type
     */
    public ByteArrayDataSource(byte[] data, String type) {
        this.data = data;
        this.type = type;
    }

    /**
     * Return an InputStream for the data.
     * Note that a new stream is returned each time
     * this method is called.
     *
     * @return the InputStream
     * @throws IOException if no data has been set
     */
    @Override
    public InputStream getInputStream() throws IOException {
        if (data == null) {
            throw new IOException("no data");
        }
        if (len < 0) {
            len = data.length;
        }
        return new ByteArrayInputStream(data, 0, len);
    }

    /**
     * Return an OutputStream for the data.
     * Writing the data is not supported; an <code>IOException</code>
     * is always thrown.
     *
     * @throws IOException always
     */
    @Override
    public OutputStream getOutputStream() throws IOException {
        throw new IOException("cannot do this");
    }

    /**
     * Get the MIME content type of the data.
     *
     * @return the MIME type
     */
    @Override
    public String getContentType() {
        return type;
    }

    /**
     * Get the name of the data.
     * By default, an empty string ("") is returned.
     *
     * @return the name of this data
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Set the name of the data.
     *
     * @param name the name of this data
     */
    public void setName(String name) {
        this.name = name;
    }
}
