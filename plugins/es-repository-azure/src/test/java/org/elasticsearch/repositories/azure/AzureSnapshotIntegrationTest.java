/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.repositories.azure;

import com.sun.net.httpserver.HttpServer;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.plugins.Plugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

public class AzureSnapshotIntegrationTest extends SQLTransportIntegrationTest {

    private static final String CONTAINER_NAME = "crate_snapshots";

    private HttpServer httpServer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AzureRepositoryPlugin.class);
        return plugins;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10001), 0);
        httpServer.createContext("/" + CONTAINER_NAME, new AzureHttpHandler(CONTAINER_NAME));
        httpServer.start();
    }

    @After
    public void tearDown() throws Exception {
        httpServer.stop(1);
        super.tearDown();
    }

    @Test
    public void create_azure_snapshot_and_restore_it() throws Exception {
        execute("CREATE TABLE t1 (x int)");
        assertThat(response.rowCount(), is(1L));

        var numberOfDocs = randomLongBetween(0, 10);
        for (int i = 0; i < numberOfDocs; i++) {
            execute("INSERT INTO t1 (x) VALUES (?)", new Object[]{randomInt()});
        }
        execute("REFRESH TABLE t1");

        execute("CREATE REPOSITORY r1 TYPE AZURE WITH (" +
                "container = '" + CONTAINER_NAME + "', " +
                "account = 'devstoreaccount1', " +
                "key = 'ZGV2c3RvcmVhY2NvdW50MQ==', " +
                "endpoint_suffix = 'ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + "')");
        assertThat(response.rowCount(), is(1L));

        execute("CREATE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)");

        execute("DROP TABLE t1");

        execute("RESTORE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)");

        execute("SELECT COUNT(*) FROM t1");
        assertThat(response.rows()[0][0], is(numberOfDocs));
    }

    private String httpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }
}
