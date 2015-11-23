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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.DataStoreConf;
import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.connection.auth.AuthResponse;
import org.irods.jargon.core.exception.AuthenticationException;
import org.irods.jargon.core.exception.DataNotFoundException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.IRODSAccessObjectFactory;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.IRODSGenQueryExecutor;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.query.IRODSGenQuery;
import org.irods.jargon.core.query.IRODSQueryResultRow;
import org.irods.jargon.core.query.IRODSQueryResultSet;
import org.irods.jargon.core.query.JargonQueryException;
import org.irods.jargon.core.query.RodsGenQueryEnum;

/**
 *
 * @author iychoi
 */
public class DataStoreClient implements Closeable {

    private static final Log LOG = LogFactory.getLog(DataStoreClient.class);

    private DataStoreConf datastoreConf;
    private IRODSFileSystem irodsFS;
    private IRODSAccount irodsAccount;
    private IRODSAccessObjectFactory accessObjectFactory;
    private IRODSGenQueryExecutor irodsGenQueryExecutor;
    private Thread keepAliveThread;

    public DataStoreClient(DataStoreConf datastoreConf) {
        if (datastoreConf == null) {
            throw new IllegalArgumentException("datastoreConf is null");
        }

        initialize(datastoreConf);
    }

    private void initialize(DataStoreConf datastoreConf) {
        this.datastoreConf = datastoreConf;
    }

    private String makeDefaultHome(String zone) {
        String defaultdir = "/" + zone.trim();
        return defaultdir;
    }

    private String makeDefaultStorageResource() {
        return "";
    }

    private IRODSAccount createIRODSAccount(String host, int port, String zone, String user, String password) throws IOException {
        IRODSAccount account = null;
        String home = makeDefaultHome(zone);
        String resource = makeDefaultStorageResource();

        try {
            account = IRODSAccount.instance(host, port, user, password, home, zone, resource);
            return account;
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }

    public synchronized void connect() throws IOException {
        try {
            this.irodsFS = IRODSFileSystem.instance();
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
        this.irodsAccount = createIRODSAccount(this.datastoreConf.getHostname(), this.datastoreConf.getPort(),
                this.datastoreConf.getZone(), this.datastoreConf.getUserId(), this.datastoreConf.getUserPwd());

        AuthResponse response;
        try {
            response = this.irodsFS.getIRODSAccessObjectFactory().authenticateIRODSAccount(this.irodsAccount);
            LOG.info("irods client connected - " + this.datastoreConf.getHostname() + ":" + this.datastoreConf.getPort());

            this.accessObjectFactory = this.irodsFS.getIRODSAccessObjectFactory();
            this.irodsGenQueryExecutor = accessObjectFactory.getIRODSGenQueryExecutor(this.irodsAccount);
        } catch (AuthenticationException ex) {
            LOG.error("Exception occurred while logging in", ex);
            throw new IOException(ex);
        } catch (JargonException ex) {
            throw new IOException(ex);
        }

        if (!response.isSuccessful()) {
            throw new IOException("Cannot authenticate to IRODS");
        }
        
        Runnable keepAliveWorker = new Runnable() {

            @Override
            public void run() {
                while(true) {
                    sendKeepAlive();
                    try {
                        Thread.sleep(5*60*1000);
                    } catch (InterruptedException ex) {
                        LOG.error("keep alive worker got interrupted", ex);
                        break;
                    }
                }
            }
            
        };
        this.keepAliveThread = new Thread(keepAliveWorker);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if(this.keepAliveThread != null) {
                this.keepAliveThread.interrupt();
                this.keepAliveThread = null;
            }
            this.irodsFS.close();
        } catch (JargonException ex) {
            throw new IOException(ex);
        }
    }

    private synchronized void sendKeepAlive() {
        try {
            IRODSFile instanceIRODSFile = this.accessObjectFactory.getIRODSFileFactory(irodsAccount).instanceIRODSFile("/");
            if(instanceIRODSFile.exists()) {
                // okey
            } else {
                LOG.error("disconnected?");
            }
        } catch (JargonException ex) {
            LOG.error("failed to send keep alive", ex);
        }
    }
    
    private synchronized IRODSQueryResultSet queryDataObjectUUID(String entity) throws JargonException, JargonQueryException {
        StringBuilder q = new StringBuilder();
        q.append("select ");
        q.append(RodsGenQueryEnum.COL_COLL_NAME.getName()).append(", ");
        q.append(RodsGenQueryEnum.COL_DATA_NAME.getName());
        q.append(" where ");
        q.append(RodsGenQueryEnum.COL_META_DATA_ATTR_NAME.getName());
        q.append(" = ");
        q.append("'ipc_UUID'");
        q.append(" AND ");
        q.append(RodsGenQueryEnum.COL_META_DATA_ATTR_VALUE.getName());
        q.append(" = ");
        q.append("'").append(entity).append("'");

        IRODSGenQuery irodsQuery = IRODSGenQuery.instance(q.toString(), 1);
        IRODSQueryResultSet resultSet = this.irodsGenQueryExecutor.executeIRODSQuery(irodsQuery, 0);
        return resultSet;
    }
    
    private synchronized IRODSQueryResultSet queryCollectionUUID(String entity) throws JargonException, JargonQueryException {
        StringBuilder q = new StringBuilder();
        q.append("select ");
        q.append(RodsGenQueryEnum.COL_COLL_NAME.getName());
        q.append(" where ");
        q.append(RodsGenQueryEnum.COL_META_COLL_ATTR_NAME.getName());
        q.append(" = ");
        q.append("'ipc_UUID'");
        q.append(" AND ");
        q.append(RodsGenQueryEnum.COL_META_COLL_ATTR_VALUE.getName());
        q.append(" = ");
        q.append("'").append(entity).append("'");

        IRODSGenQuery irodsQuery = IRODSGenQuery.instance(q.toString(), 1);
        IRODSQueryResultSet resultSet = this.irodsGenQueryExecutor.executeIRODSQuery(irodsQuery, 0);
        return resultSet;
    }

    public synchronized String convertUUIDToPath(String entity) throws IOException {
        // test dataobject
        try {
            IRODSQueryResultSet dataObjectResult = queryDataObjectUUID(entity);
            IRODSQueryResultRow firstDataObjectResult = dataObjectResult.getFirstResult();
            if(firstDataObjectResult != null) {
                String parent = firstDataObjectResult.getColumn(0);
                String file = firstDataObjectResult.getColumn(1);
                if(parent.endsWith("/")) {
                    return parent + file;
                } else {
                    return parent + "/" + file;
                }
            }
        } catch (DataNotFoundException ex) {
            // fall
        } catch (JargonException ex) {
            LOG.error("Exception occurred while querying", ex);
            throw new IOException(ex);
        } catch (JargonQueryException ex) {
            LOG.error("Exception occurred while querying", ex);
            throw new IOException(ex);
        }
        
        // test collection
        try {
            IRODSQueryResultSet collectionResult = queryCollectionUUID(entity);
            IRODSQueryResultRow firstCollectionResult = collectionResult.getFirstResult();
            if(firstCollectionResult != null) {
                String parent = firstCollectionResult.getColumn(0);
                return parent;
            }
        } catch (DataNotFoundException ex) {
            // fall
        } catch (JargonException ex) {
            LOG.error("Exception occurred while querying", ex);
            throw new IOException(ex);
        } catch (JargonQueryException ex) {
            LOG.error("Exception occurred while querying", ex);
            throw new IOException(ex);
        }
        
        return null;
    }
}
