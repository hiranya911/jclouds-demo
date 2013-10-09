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
import com.google.common.collect.Iterables;
import com.google.inject.Module;
import org.apache.log4j.Logger;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.*;
import org.jclouds.compute.predicates.OperatingSystemPredicates;
import org.jclouds.io.Payloads;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.ssh.SshClient;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.util.Properties;

public class EC2Demo {

    private static final Logger log = Logger.getLogger(EC2Demo.class);

    public static final String CLOUD_PROVIDER = "compute.provider";
    public static final String CLOUD_ACCESS_KEY = "compute.access.key";
    public static final String CLOUD_SECRET_KEY = "compute.secret.key";
    public static final String CLOUD_IMAGE = "compute.image";
    public static final String CLOUD_REGION = "compute.region";

    public static void main(String[] args) {
        ComputeServiceContext context = null;
        NodeMetadata node = null;
        SshClient client = null;

        try {
            Properties properties = Utils.loadProperties();
            context = initContext(properties);
            node = startNode(context, properties);

            client = context.utils().sshForNode().apply(node);
            if (client != null) {
                System.out.println("===============================================");
                client.connect();
                client.put("calculate.py", Payloads.newFilePayload(new File(
                        "/Users/hiranya/academic/cloud_290b/projects/jclouds-demo/calculate.py")));
                client.put("s3client.zip", Payloads.newFilePayload(new File(
                        "/Users/hiranya/academic/cloud_290b/projects/s3client.zip")));

                runCommand(client, "uname -a");
                if (OperatingSystemPredicates.supportsApt().apply(node.getOperatingSystem())) {
                    runCommand(client, "sudo apt-get update");
                    runCommand(client, "sudo apt-get install -y unzip python openjdk-6-jre-headless");
                }
                runCommand(client, "unzip s3client.zip");
                runCommand(client, "sh s3client/bin/run.sh get input.dat");

                String outputFile = "output-" + System.currentTimeMillis() + ".txt";
                runCommand(client, "python calculate.py input.dat > " + outputFile);
                runCommand(client, "sh s3client/bin/run.sh put " + outputFile);
                runCommand(client, "cat " + outputFile);
                client.disconnect();
                System.out.println("===============================================");
            }

        } catch (Exception e) {
            System.err.println("Unexpected exception encountered.");
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.disconnect();
            }
            if (node != null) {
                stopNode(context, node);
            }
            if (context != null) {
                context.close();
            }
        }
    }

    private static ComputeServiceContext initContext(Properties properties) {
        log.debug("Initializing compute service context...");
        return ContextBuilder.newBuilder(properties.getProperty(CLOUD_PROVIDER)).
                credentials(
                        properties.getProperty(CLOUD_ACCESS_KEY),
                        properties.getProperty(CLOUD_SECRET_KEY)).
                modules(ImmutableSet.<Module>of(
                        new Log4JLoggingModule(),
                        new SshjSshClientModule())).
                buildView(ComputeServiceContext.class);
    }

    private static NodeMetadata startNode(ComputeServiceContext context,
                                          Properties properties) throws RunNodesException {
        String region = properties.getProperty(CLOUD_REGION);
        String image = properties.getProperty(CLOUD_IMAGE);
        ComputeService computeService = context.getComputeService();
        Template template;
        if (image != null) {
            template = computeService.templateBuilder().
                    imageId(region + "/" + image).
                    smallest().
                    build();
        } else {
            template = computeService.templateBuilder().
                    osFamily(OsFamily.UBUNTU).
                    osVersionMatches("12.04").
                    locationId(region).
                    smallest().
                    build();
        }
        NodeMetadata node = Iterables.getOnlyElement(computeService.createNodesInGroup(
                "jclouds", 1, template));
        if (log.isDebugEnabled()) {
            log.debug("New node started successfully.");
            log.debug("Node ID: " + node.getId());
            log.debug("Node Image: " + node.getImageId());
            log.debug("Node OS: " + node.getOperatingSystem().getFamily().value() +
                    "-" + node.getOperatingSystem().getVersion());
            log.debug("Node Hostname: " + node.getHostname());
        }
        return node;
    }

    private static void stopNode(ComputeServiceContext context, NodeMetadata node) {
        context.getComputeService().destroyNode(node.getId());
    }

    private static void runCommand(SshClient client, String command) {
        System.out.println("$ " + command);
        ExecResponse response = client.exec(command);
        System.out.print(response.getOutput());
        if (response.getExitStatus() != 0) {
            System.err.println(response.getError());
            throw new RuntimeException("Error while running the command: " + command);
        }
    }
}
