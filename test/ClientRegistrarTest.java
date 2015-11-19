/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.iplantcollaborative.ClientRegistrar;
import org.iplantcollaborative.IPlantBorderMessageServer;
import org.iplantcollaborative.conf.BMSConf;
import org.iplantcollaborative.lease.Client;
import org.iplantcollaborative.lease.msg.RequestLease;
import org.iplantcollaborative.lease.msg.ResponseLease;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author iychoi
 */
public class ClientRegistrarTest {
    
    private BMSConf conf;
    private IPlantBorderMessageServer server;
    
    public ClientRegistrarTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() throws IOException {
        BasicConfigurator.configure();
        this.conf = BMSConf.createInstance(new File("./config.json"));
        this.server = new IPlantBorderMessageServer(this.conf.getMessageServerConf(), this.conf.getDataStoreConfConf());
        this.server.connect();
    }
    
    @After
    public void tearDown() throws IOException {
        this.server.close();
        this.server = null;
        this.conf = null;
    }

    @Test
    public void testLease() throws InterruptedException, IOException {
        ClientRegistrar registrar = this.server.getRegistrar();
        RequestLease req = new RequestLease();
        Client client = new Client();
        client.setApplicationName("AGTest");
        client.setUserId("iychoi");
        req.setClient(client);
        
        ResponseLease res = registrar.lease(req);
        
        Assert.assertNotNull(res);
        Assert.assertEquals("client obj", req.getClient(), res.getClient());
    }
}
