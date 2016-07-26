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
import org.iplantcollaborative.IPlantBorderMessageServer;
import org.iplantcollaborative.conf.BMSConf;
import org.iplantcollaborative.irods.DataStoreClient;
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
public class DataStoreClientTest {
    
    private boolean test;
    private BMSConf conf;
    private IPlantBorderMessageServer server;
    
    public DataStoreClientTest() {
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
        File f = new File("./test_config.json");
        this.test = false;
        
        if(f.exists()) {
            this.test = true;
            this.conf = BMSConf.createInstance(new File("./test_config.json"));
            this.server = new IPlantBorderMessageServer(this.conf.getMessageServerConf(), this.conf.getDataStoreConfConf());
            this.server.connect();
        }
    }
    
    @After
    public void tearDown() throws IOException {
        if(this.test) {
            this.server.close();
            this.server = null;
            this.conf = null;
        }
    }

    @Test
    public void testUUIDConvDataObject() throws InterruptedException, IOException {
        if(this.test) {
            DataStoreClient datastoreClient = this.server.getProcessor().getDatastoreClient();
            String dataObjectPath = datastoreClient.convertUUIDToPathForDataObject("83015514-8db6-11e5-9d66-1a5a300ff36f");
            Assert.assertNotNull(dataObjectPath);
            Assert.assertEquals("uuid of an object", "/iplant/home/iychoi/test.txt", dataObjectPath);
        }
    }
    
    @Test
    public void testUUIDConvCollection() throws InterruptedException, IOException {
        if(this.test) {
            DataStoreClient datastoreClient = this.server.getProcessor().getDatastoreClient();
            String collectionPath = datastoreClient.convertUUIDToPathForCollection("6d7da718-8db8-11e5-9d66-1a5a300ff36f");
            Assert.assertNotNull(collectionPath);
            Assert.assertEquals("uuid of a collection", "/iplant/home/iychoi/testColl", collectionPath);
        }
    }
    
    @Test
    public void testUUIDConvNotExist() throws InterruptedException, IOException {
        if(this.test) {
            DataStoreClient datastoreClient = this.server.getProcessor().getDatastoreClient();
            String notExistUUID = datastoreClient.convertUUIDToPathForDataObject("6d7da718-8db8-11e5-9d66-1a5a300ffd6f");
            Assert.assertNull(notExistUUID);
        }
    }
    
}
