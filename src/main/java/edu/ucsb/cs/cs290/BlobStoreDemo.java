/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package edu.ucsb.cs.cs290;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

public class BlobStoreDemo {

    public static final String BLOB_PROVIDER = "blob.provider";
    public static final String BLOB_ACCESS_KEY = "blob.access.key";
    public static final String BLOB_SECRET_KEY = "blob.secret.key";
    public static final String BLOB_CONTAINER = "blob.container";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: BlobStoreDemo get|put file");
            return;
        }

        BlobStoreContext context = null;
        try {
            Properties properties = Utils.loadProperties();
            context = initContext(properties);
            BlobStore blobStore = context.getBlobStore();

            String fileName = args[1];

            if ("get".equals(args[0])) {
                // Download the specified file from the cloud
                Blob blob = blobStore.getBlob(properties.getProperty(BLOB_CONTAINER), fileName);
                FileOutputStream outputStream = new FileOutputStream(fileName);
                blob.getPayload().writeTo(outputStream);
                outputStream.flush();
                outputStream.close();
                System.out.println("Downloaded blob: " + fileName);
            } else if ("put".equals(args[0])) {
                // Upload the specified file to the cloud
                File file = new File(fileName);
                Blob blob = blobStore.blobBuilder(file.getName()).payload(file).build();
                blobStore.putBlob(properties.getProperty(BLOB_CONTAINER), blob);
                System.out.println("Uploaded blob: " + fileName);
            } else {
                System.err.println("Unsupported command: " + args[0]);
            }
        } catch (Exception e) {
            System.err.println("Unexpected exception encountered.");
            e.printStackTrace();
        } finally {
            if (context != null) {
                context.close();
            }
        }
    }

    private static BlobStoreContext initContext(Properties properties) {
        return ContextBuilder.newBuilder(properties.getProperty(BLOB_PROVIDER))
                .credentials(
                        properties.getProperty(BLOB_ACCESS_KEY),
                        properties.getProperty(BLOB_SECRET_KEY))
                .modules(ImmutableSet.<Module>of(new Log4JLoggingModule()))
                .buildView(BlobStoreContext.class);
    }
}
