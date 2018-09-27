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

package com.aliyun.oss.common.utils;

import com.aliyun.oss.common.comm.io.BoundedInputStream;
import com.aliyun.oss.common.comm.io.RepeatableBoundedFileInputStream;
import com.aliyun.oss.common.comm.io.RepeatableFileInputStream;
import com.aliyun.oss.internal.OSSConstants;

import java.io.*;
import java.util.zip.CheckedInputStream;

public class IOUtils {

    public static byte[] readStreamAsByteArray(InputStream in)
            throws IOException {
        
        if (in == null) {
            return new byte[0];
        }
        
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = -1;
        while ((len = in.read(buffer)) != -1) {
            output.write(buffer, 0, len);
        }
        output.flush();
        return output.toByteArray();
    }

    public static void safeClose(InputStream inputStream) {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ignored) {}
        }
    }

    public static void safeClose(OutputStream outputStream) {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException ignored) {}
        }
    }
    
    public static boolean checkFile(File file) {
        if (file == null) {
            return false;
        }
        
        boolean exists;
        boolean isFile;
        boolean canRead;
        try {
            exists = file.exists();
            isFile = file.isFile();
            canRead = file.canRead();
        } catch (SecurityException se) {
            // Swallow the exception and return false directly.
            return false;
        }
        
        return (exists && isFile && canRead);
    }
    
    public static InputStream newRepeatableInputStream(final InputStream original) throws IOException {
        InputStream repeatable;
        if (!original.markSupported()) {
            if (original instanceof FileInputStream) {
                repeatable = new RepeatableFileInputStream((FileInputStream)original);
            } else {
                repeatable = new BufferedInputStream(original, OSSConstants.DEFAULT_STREAM_BUFFER_SIZE);                
            }
        } else {
            repeatable = original;
        }
        return repeatable;
    }
    
    public static InputStream newRepeatableInputStream(final BoundedInputStream original) throws IOException {
        InputStream repeatable;
        if (!original.markSupported()) {
            if (original.getWrappedInputStream() instanceof FileInputStream) {
                repeatable = new RepeatableBoundedFileInputStream(original);
            } else {
                repeatable = new BufferedInputStream(original, OSSConstants.DEFAULT_STREAM_BUFFER_SIZE);
            }
        } else {
            repeatable = original;
        }
        return repeatable;
    }
    
    public static Long getCRCValue(InputStream inputStream) {
        if (inputStream instanceof CheckedInputStream) {
            return ((CheckedInputStream) inputStream).getChecksum().getValue();
        }
        return null;
    }
    
}
