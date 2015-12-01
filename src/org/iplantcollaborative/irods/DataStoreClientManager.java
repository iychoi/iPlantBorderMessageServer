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
package org.iplantcollaborative.irods;

import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.DataStoreConf;

/**
 *
 * @author iychoi
 */
public class DataStoreClientManager implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(DataStoreClientManager.class);
    
    private DataStoreConf datastoreConf;
    private DataStoreClient currentDatastoreClient;
    
    public DataStoreClientManager(DataStoreConf datastoreConf) {
        if(datastoreConf == null) {
            throw new IllegalArgumentException("datastoreConf is null");
        }
        
        this.datastoreConf = datastoreConf;
    }
    
    private DataStoreClient createNewConnection() throws IOException {
        DataStoreClient dataStoreClient = new DataStoreClient(this.datastoreConf);
        dataStoreClient.connect();
        
        return dataStoreClient;
    }
    
    public synchronized DataStoreClient getDatastoreClientInstance() throws IOException {
        if(this.currentDatastoreClient == null) {
            this.currentDatastoreClient = createNewConnection();
            return this.currentDatastoreClient;
        }
        
        // check alive
        return this.currentDatastoreClient;
    }
    
    public synchronized DataStoreClient getDatastoreClientInstance(boolean newconn) throws IOException {
        if(this.currentDatastoreClient == null) {
            this.currentDatastoreClient = createNewConnection();
            return this.currentDatastoreClient;
        }
        
        if(newconn) {
            refreshClient();
        }
        
        return this.currentDatastoreClient;
    }
    
    public synchronized void refreshClient() {
        if(this.currentDatastoreClient != null) {
            try {
                this.currentDatastoreClient.close();
            } catch (IOException ex) {
                LOG.info(ex);
            }
            this.currentDatastoreClient = null;
        }
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.currentDatastoreClient != null) {
            this.currentDatastoreClient.close();
            this.currentDatastoreClient = null;
        }
    }
}
