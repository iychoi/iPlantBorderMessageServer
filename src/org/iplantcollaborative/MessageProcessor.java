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
package org.iplantcollaborative;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.AccessPermissionCache.AccessPermissionKey;
import org.iplantcollaborative.conf.DataStoreConf;
import org.iplantcollaborative.datastore.msg.ADataStoreMessage;
import org.iplantcollaborative.datastore.msg.CollectionAclMod;
import org.iplantcollaborative.datastore.msg.CollectionAdd;
import org.iplantcollaborative.datastore.msg.CollectionMetadataAdd;
import org.iplantcollaborative.datastore.msg.CollectionMv;
import org.iplantcollaborative.datastore.msg.CollectionRm;
import org.iplantcollaborative.datastore.msg.DataObjectAclMod;
import org.iplantcollaborative.datastore.msg.DataObjectAdd;
import org.iplantcollaborative.datastore.msg.DataObjectMetadataAdd;
import org.iplantcollaborative.datastore.msg.DataObjectMetadataMod;
import org.iplantcollaborative.datastore.msg.DataObjectMod;
import org.iplantcollaborative.datastore.msg.DataObjectMv;
import org.iplantcollaborative.datastore.msg.DataObjectRm;
import org.iplantcollaborative.irods.DataStoreClient;
import org.iplantcollaborative.irods.DataStoreClientManager;
import org.iplantcollaborative.lease.Client;
import org.iplantcollaborative.utils.JsonSerializer;
import org.iplantcollaborative.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class MessageProcessor implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(MessageProcessor.class);
    
    private Binder binder;
    private JsonSerializer serializer;
    private DataStoreClientManager datastoreClientManager;
    private UUIDCache uuidCache;
    private AccessPermissionCache accessPermissionCache;
    
    public MessageProcessor(DataStoreConf datastoreConf, Binder binder) {
        if(datastoreConf == null) {
            throw new IllegalArgumentException("datastoreConf is null");
        }
        
        if(binder == null) {
            throw new IllegalArgumentException("binder is null");
        }
        
        this.binder = binder;
        
        binder.setProcessor(this);
        
        this.serializer = new JsonSerializer();
        this.uuidCache = new UUIDCache();
        this.accessPermissionCache = new AccessPermissionCache();
        
        this.datastoreClientManager = new DataStoreClientManager(datastoreConf);
    }
    
    public void connect() throws IOException {
    }
    
    public void process(String routingKey, String message) {
        try {
            ADataStoreMessage dsMsg = createJsonMessageObject(routingKey, message);
            if(dsMsg != null) {
                if(this.binder.getClientRegistrar() != null) {
                    List<Client> acceptedClients = listAcceptedClients(dsMsg);
                    if(!acceptedClients.isEmpty()) {
                        Message msg = new Message();
                        String msgbody = this.serializer.toJson(dsMsg);
                        
                        msg.addRecipient(acceptedClients);
                        msg.setMessageBody(msgbody);
                        
                        MessagePublisher publisher = this.binder.getPublisher();
                        if(publisher != null) {
                            try {
                                publisher.publish(msg);
                            } catch (IOException ex) {
                                LOG.error("Exception occurred while publishing a message", ex);
                            }
                        } else {
                            LOG.error("processor not registered");
                        }
                    }
                } else {
                    LOG.error("client registrar not registered");
                }
            } else {
                LOG.info("cannot process a message " + routingKey);
            }
        } catch (Exception ex) {
            LOG.info(message);
            LOG.error("Exception occurred while processing a message", ex);
        }
    }
    
    private ADataStoreMessage createJsonMessageObject(String routingKey, String message) throws IOException {
        ADataStoreMessage dsMsg = null;
        switch(routingKey) {
            case "collection.add":
            {
                CollectionAdd msg = (CollectionAdd) this.serializer.fromJson(message, CollectionAdd.class);
                msg.setEntityPath(msg.getPath());
                dsMsg = msg;
            }
                break;
            case "collection.rm":
            {
                CollectionRm msg = (CollectionRm) this.serializer.fromJson(message, CollectionRm.class);
                msg.setEntityPath(msg.getPath());
                dsMsg = msg;
            }
                break;
            case "collection.mv":
            {
                CollectionMv msg = (CollectionMv) this.serializer.fromJson(message, CollectionMv.class);
                msg.setEntityPath(msg.getNewPath());
                dsMsg = msg;
            }
                break;
            case "collection.acl.mod":
            {
                CollectionAclMod msg = (CollectionAclMod) this.serializer.fromJson(message, CollectionAclMod.class);
                String entityPath = convertUUIDToPathForCollection(msg.getEntity());
                msg.setEntityPath(entityPath);
                dsMsg = msg;
            }
                break;
            case "collection.sys-metadata.add":
            {
                CollectionMetadataAdd msg = (CollectionMetadataAdd) this.serializer.fromJson(message, CollectionMetadataAdd.class);
                String entityPath = convertUUIDToPathForCollection(msg.getEntity());
                msg.setEntityPath(entityPath);
                dsMsg = msg;
            }
                break;
            case "data-object.add":
            {
                DataObjectAdd msg = (DataObjectAdd) this.serializer.fromJson(message, DataObjectAdd.class);
                msg.setEntityPath(msg.getPath());
                dsMsg = msg;
            }
                break;
            case "data-object.rm":
            {
                DataObjectRm msg = (DataObjectRm) this.serializer.fromJson(message, DataObjectRm.class);
                msg.setEntityPath(msg.getPath());
                dsMsg = msg;
            }
                break;
            case "data-object.mod":
            {
                DataObjectMod msg = (DataObjectMod) this.serializer.fromJson(message, DataObjectMod.class);
                String entityPath = convertUUIDToPathForDataObject(msg.getEntity());
                msg.setEntityPath(entityPath);
                dsMsg = msg;
            }
                break;
            case "data-object.mv":
            {
                DataObjectMv msg = (DataObjectMv) this.serializer.fromJson(message, DataObjectMv.class);
                msg.setEntityPath(msg.getNewPath());
                dsMsg = msg;
            }
                break;
            case "data-object.acl.mod":
            {
                DataObjectAclMod msg = (DataObjectAclMod) this.serializer.fromJson(message, DataObjectAclMod.class);
                String entityPath = convertUUIDToPathForDataObject(msg.getEntity());
                msg.setEntityPath(entityPath);
                dsMsg = msg;
            }
                break;
            case "data-object.sys-metadata.add":
            {
                DataObjectMetadataAdd msg = (DataObjectMetadataAdd) this.serializer.fromJson(message, DataObjectMetadataAdd.class);
                String entityPath = convertUUIDToPathForDataObject(msg.getEntity());
                msg.setEntityPath(entityPath);
                dsMsg = msg;
            }
                break;
            case "data-object.sys-metadata.mod":
            {
                DataObjectMetadataMod msg = (DataObjectMetadataMod) this.serializer.fromJson(message, DataObjectMetadataMod.class);
                String entityPath = convertUUIDToPathForDataObject(msg.getEntity());
                msg.setEntityPath(entityPath);
                dsMsg = msg;
            }
                break;
            default:
            {
                LOG.info("cannot find datastore data object matching to a message " + routingKey);
                LOG.info(message);
                dsMsg = null;
            }
                break;
        }
        
        if(dsMsg != null) {
            // cache uuid-path if necessary
            if(dsMsg.getEntity() != null && !dsMsg.getEntity().isEmpty() && 
                    dsMsg.getEntityPath() != null && !dsMsg.getEntityPath().isEmpty()) {
                this.uuidCache.cache(dsMsg.getEntity(), dsMsg.getEntityPath());
            }
            
            // set operation
            dsMsg.setOperation(routingKey);
        }
        
        return dsMsg;
    }
    
    private String convertUUIDToPathForDataObject(String entity) throws IOException {
        String cachedPath = this.uuidCache.get(entity);
        if(cachedPath != null || !cachedPath.isEmpty()) {
            return cachedPath;
        }
        
        try {
            DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
            String path = datastoreClientInstance.convertUUIDToPathForDataObject(entity);
            return path;
        } catch (IOException ex) {
            DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance(true);
            String path = datastoreClientInstance.convertUUIDToPathForDataObject(entity);
            return path;
        }
    }
    
    private String convertUUIDToPathForCollection(String entity) throws IOException {
        String cachedPath = this.uuidCache.get(entity);
        if(cachedPath != null || !cachedPath.isEmpty()) {
            return cachedPath;
        }
        
        try {
            DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
            String path = datastoreClientInstance.convertUUIDToPathForCollection(entity);
            return path;
        } catch (IOException ex) {
            DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance(true);
            String path = datastoreClientInstance.convertUUIDToPathForCollection(entity);
            return path;
        }
    }
    
    private boolean _checkAccessPermissionForCollection(DataStoreClient datastoreClientInstance, String path, String userId) throws IOException {
        try {
            AccessPermissionKey accessPermissionKey = new AccessPermissionKey(userId, path);
            Boolean cachedAccessPermission = this.accessPermissionCache.get(accessPermissionKey);
            if(cachedAccessPermission != null) {
                return cachedAccessPermission.booleanValue();
            }
            
            boolean bAccessPermission = datastoreClientInstance.hasAccessPermissionsForCollection(path, userId);
            this.accessPermissionCache.cache(accessPermissionKey, bAccessPermission);
            
            return bAccessPermission;
        } catch (FileNotFoundException ex) {
            String parentPath = PathUtils.getParentPath(path);
            if(parentPath != null) {
                return _checkAccessPermissionForCollection(datastoreClientInstance, parentPath, userId);
            }
            return false;
        }
    }
    
    private boolean checkAccessPermissionForCollection(String path, String userId) throws IOException {
        try {
            DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
            return _checkAccessPermissionForCollection(datastoreClientInstance, path, userId);
        } catch (IOException ex) {
            DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance(true);
            return _checkAccessPermissionForCollection(datastoreClientInstance, path, userId);
        }
    }
    
    private List<Client> listAcceptedClients(ADataStoreMessage msg) throws IOException {
        List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(msg);
        Map<String, Boolean> userAcceptance = new HashMap<String, Boolean>();
        List<Client> acceptedClients = new ArrayList<Client>();
        String path = PathUtils.getParentPath(msg.getEntityPath());
        
        for(Client client : clients) {
            Boolean baccept = userAcceptance.get(client.getUserId());
            if(baccept == null) {
                if(checkAccessPermissionForCollection(path, client.getUserId())) {
                    userAcceptance.put(client.getUserId(), true);
                    acceptedClients.add(client);
                } else {
                    userAcceptance.put(client.getUserId(), false);
                }
            } else {
                if(baccept.booleanValue()) {
                    acceptedClients.add(client);
                }
            }
        }
        return acceptedClients;
    }
    
    public DataStoreClient getDatastoreClient() throws IOException {
        return this.datastoreClientManager.getDatastoreClientInstance(true);
    }

    @Override
    public void close() throws IOException {
        this.datastoreClientManager.close();
        this.datastoreClientManager = null;
        
        this.uuidCache.clearCache();
        this.accessPermissionCache.clearCache();
    }
}
