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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.DataStoreConf;
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

/**
 *
 * @author iychoi
 */
public class MessageProcessor implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(MessageProcessor.class);
    
    private Binder binder;
    private JsonSerializer serializer;
    private DataStoreClientManager datastoreClientManager;
    private UUIDCache cache;
    
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
        this.cache = new UUIDCache();
        
        this.datastoreClientManager = new DataStoreClientManager(datastoreConf);
    }
    
    public void connect() throws IOException {
    }
    
    public void process(String routingKey, String message) {
        Message msg = null;
        try {
            switch(routingKey) {
                case "collection.add":
                    msg = process_collection_add(routingKey, message);
                    break;
                case "collection.rm":
                    msg = process_collection_rm(routingKey, message);
                    break;
                case "collection.mv":
                    msg = process_collection_mv(routingKey, message);
                    break;
                case "collection.acl.mod":
                    msg = process_collection_acl_mod(routingKey, message);
                    break;
                case "collection.sys-metadata.add":
                    msg = process_collection_metadata_add(routingKey, message);
                    break;
                case "data-object.add":
                    msg = process_dataobject_add(routingKey, message);
                    break;
                case "data-object.rm":
                    msg = process_dataobject_rm(routingKey, message);
                    break;
                case "data-object.mod":
                    msg = process_dataobject_mod(routingKey, message);
                    break;
                case "data-object.mv":
                    msg = process_dataobject_mv(routingKey, message);
                    break;
                case "data-object.acl.mod":
                    msg = process_dataobject_acl_mod(routingKey, message);
                    break;
                case "data-object.sys-metadata.add":
                    msg = process_dataobject_metadata_add(routingKey, message);
                    break;
                case "data-object.sys-metadata.mod":
                    msg = process_dataobject_metadata_mod(routingKey, message);
                    break;
                default:
                    LOG.info("message has no processor - ignored - " + routingKey);
                    LOG.info(message);
                    break;
            }

            if(msg != null) {
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
        } catch (Exception ex) {
            LOG.info(message);
            LOG.error("Exception occurred while processing a message", ex);
        }
    }
    /*
    private String extractUserNameFromPath(String path) {
        if(path == null || path.isEmpty()) {
            return null;
        }
        
        int idx = path.indexOf("/home/");
        if(idx >= 0) {
            String tail = path.substring(idx + 6);
            int end = tail.indexOf("/");
            if(end >= 0) {
                tail = tail.substring(0, end);
            }
            return tail;
        }
        return null;
    }
    
    private String extractClientUserIdFromAuthor(User author) {
        return author.getName();
    }
    
    private String extractClientUserIdFromPath(String path) {
        return extractUserNameFromPath(path);
    }

    private String[] extractClientUserIdsFromReaders(User reader) {
        String[] userIds = new String[1];
        
        String userId = extractClientUserIdFromAuthor(reader);
        userIds[0] = userId;
        
        return userIds;
    }
    
    private String[] extractClientUserIdsFromReaders(List<User> readers) {
        String[] userIds = new String[readers.size()];
        
        int i=0;
        for(User reader : readers) {
            String userId = extractClientUserIdFromAuthor(reader);
            userIds[i] = userId;
            i++;
        }
        
        return userIds;
    }
    */
    
    private Message process_collection_add(String routingKey, String message) throws IOException {
        CollectionAdd ca = (CollectionAdd) this.serializer.fromJson(message, CollectionAdd.class);
        
        ca.setEntityPath(ca.getPath());
        
        // cache uuid-path
        if(ca.getEntity() != null && !ca.getEntity().isEmpty() && 
                ca.getEntityPath() != null && !ca.getEntityPath().isEmpty()) {
            this.cache.cache(ca.getEntity(), ca.getEntityPath());
        }
        
        if(ca.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        ca.setOperation(routingKey);
        String msgbody = this.serializer.toJson(ca);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(ca.getAuthor());
            if (author != null) {
            List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
            msg.addRecipient(clients);
            }
            String pathowner = extractClientUserIdFromPath(ca.getPath());
            if (pathowner != null && !pathowner.equals(author)) {
            List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
            msg.addRecipient(clients);
            }
             */
            List<Client> acceptedClients = listAcceptedClientsForCollection(msgbody, ca.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_collection_rm(String routingKey, String message) throws IOException {
        CollectionRm cr = (CollectionRm) this.serializer.fromJson(message, CollectionRm.class);
        
        cr.setEntityPath(cr.getPath());
        
        // cache uuid-path
        if(cr.getEntity() != null && !cr.getEntity().isEmpty() && 
                cr.getEntityPath() != null && !cr.getEntityPath().isEmpty()) {
            this.cache.cache(cr.getEntity(), cr.getEntityPath());
        }
        
        if(cr.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        cr.setOperation(routingKey);
        String msgbody = this.serializer.toJson(cr);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(cr.getAuthor());
            if (author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String pathowner = extractClientUserIdFromPath(cr.getPath());
            if (pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForCollection(msgbody, cr.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_collection_mv(String routingKey, String message) throws IOException {
        CollectionMv cm = (CollectionMv) this.serializer.fromJson(message, CollectionMv.class);
        
        cm.setEntityPath(cm.getNewPath());
        
        // cache uuid-path
        if(cm.getEntity() != null && !cm.getEntity().isEmpty() && 
                cm.getEntityPath() != null && !cm.getEntityPath().isEmpty()) {
            this.cache.cache(cm.getEntity(), cm.getEntityPath());
        }
        
        if(cm.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        cm.setOperation(routingKey);
        String msgbody = this.serializer.toJson(cm);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(cm.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String pathownerOld = extractClientUserIdFromPath(cm.getOldPath());
            if(pathownerOld != null && !pathownerOld.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathownerOld, msgbody);
                msg.addRecipient(clients);
            }

            String pathownerNew = extractClientUserIdFromPath(cm.getNewPath());
            if(pathownerNew != null && !pathownerNew.equals(author) && !pathownerNew.equals(pathownerOld)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathownerNew, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForCollection(msgbody, cm.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_collection_acl_mod(String routingKey, String message) throws IOException {
        CollectionAclMod cam = (CollectionAclMod) this.serializer.fromJson(message, CollectionAclMod.class);
        
        String entityPath = convertUUIDToPathForCollection(cam.getEntity());
        cam.setEntityPath(entityPath);
        
        // cache uuid-path
        if(cam.getEntity() != null && !cam.getEntity().isEmpty() && 
                cam.getEntityPath() != null && !cam.getEntityPath().isEmpty()) {
            this.cache.cache(cam.getEntity(), cam.getEntityPath());
        }
        
        if(cam.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        cam.setOperation(routingKey);
        String msgbody = this.serializer.toJson(cam);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(cam.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String pathowner = extractClientUserIdFromPath(cam.getEntityPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForCollection(msgbody, cam.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }
    
    private Message process_collection_metadata_add(String routingKey, String message) throws IOException {
        CollectionMetadataAdd cma = (CollectionMetadataAdd) this.serializer.fromJson(message, CollectionMetadataAdd.class);
        
        if(cma.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String entityPath = convertUUIDToPathForCollection(cma.getEntity());
        cma.setEntityPath(entityPath);
        
        // cache uuid-path
        if(cma.getEntity() != null && !cma.getEntity().isEmpty() && 
                cma.getEntityPath() != null && !cma.getEntityPath().isEmpty()) {
            this.cache.cache(cma.getEntity(), cma.getEntityPath());
        }
        
        cma.setOperation(routingKey);
        String msgbody = this.serializer.toJson(cma);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(cma.getAuthor());
            if (author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }
            
            String pathowner = extractClientUserIdFromPath(cma.getEntityPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForCollection(msgbody, cma.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_add(String routingKey, String message) throws IOException {
        DataObjectAdd doa = (DataObjectAdd) this.serializer.fromJson(message, DataObjectAdd.class);
        
        doa.setEntityPath(doa.getPath());
        
        // cache uuid-path
        if(doa.getEntity() != null && !doa.getEntity().isEmpty() && 
                doa.getEntityPath()!= null && !doa.getEntityPath().isEmpty()) {
            this.cache.cache(doa.getEntity(), doa.getEntityPath());
        }
        
        if(doa.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        doa.setOperation(routingKey);
        String msgbody = this.serializer.toJson(doa);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(doa.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String creator = extractClientUserIdFromAuthor(doa.getCreator());
            if(creator != null && !creator.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(creator, msgbody);
                msg.addRecipient(clients);
            }

            String pathowner = extractClientUserIdFromPath(doa.getPath());
            if(pathowner != null && !pathowner.equals(author) && !pathowner.equals(creator)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, doa.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_rm(String routingKey, String message) throws IOException {
        DataObjectRm dor = (DataObjectRm) this.serializer.fromJson(message, DataObjectRm.class);
        
        dor.setEntityPath(dor.getPath());
        
        // cache uuid-path
        if(dor.getEntity() != null && !dor.getEntity().isEmpty() && 
                dor.getEntityPath()!= null && !dor.getEntityPath().isEmpty()) {
            this.cache.cache(dor.getEntity(), dor.getEntityPath());
        }
        
        if(dor.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        dor.setOperation(routingKey);
        String msgbody = this.serializer.toJson(dor);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(dor.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String pathowner = extractClientUserIdFromPath(dor.getPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, dor.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_mod(String routingKey, String message) throws IOException {
        DataObjectMod dom = (DataObjectMod) this.serializer.fromJson(message, DataObjectMod.class);
        
        String entityPath = convertUUIDToPathForDataObject(dom.getEntity());
        dom.setEntityPath(entityPath);
        
        // cache uuid-path
        if(dom.getEntity() != null && !dom.getEntity().isEmpty() && 
                dom.getEntityPath()!= null && !dom.getEntityPath().isEmpty()) {
            this.cache.cache(dom.getEntity(), dom.getEntityPath());
        }
        
        if(dom.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        dom.setOperation(routingKey);
        String msgbody = this.serializer.toJson(dom);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();

            /*
            String author = extractClientUserIdFromAuthor(dom.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String creator = extractClientUserIdFromAuthor(dom.getCreator());
            if(creator != null && !creator.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(creator, msgbody);
                msg.addRecipient(clients);
            }

            String pathowner = extractClientUserIdFromPath(dom.getEntityPath());
            if(pathowner != null && !pathowner.equals(author) && !pathowner.equals(creator)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, dom.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_mv(String routingKey, String message) throws IOException {
        DataObjectMv dom = (DataObjectMv) this.serializer.fromJson(message, DataObjectMv.class);
        
        dom.setEntityPath(dom.getNewPath());
        
        // cache uuid-path
        if(dom.getEntity() != null && !dom.getEntity().isEmpty() && 
                dom.getEntityPath()!= null && !dom.getEntityPath().isEmpty()) {
            this.cache.cache(dom.getEntity(), dom.getEntityPath());
        }
        
        if(dom.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        dom.setOperation(routingKey);
        String msgbody = this.serializer.toJson(dom);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(dom.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String pathownerOld = extractClientUserIdFromPath(dom.getOldPath());
            if(pathownerOld != null && !pathownerOld.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathownerOld, msgbody);
                msg.addRecipient(clients);
            }

            String pathownerNew = extractClientUserIdFromPath(dom.getNewPath());
            if(pathownerNew != null && !pathownerNew.equals(author) && !pathownerNew.equals(pathownerOld)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathownerNew, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, dom.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_acl_mod(String routingKey, String message) throws IOException {
        DataObjectAclMod doam = (DataObjectAclMod) this.serializer.fromJson(message, DataObjectAclMod.class);
        
        String entityPath = convertUUIDToPathForDataObject(doam.getEntity());
        doam.setEntityPath(entityPath);
        
        // cache uuid-path
        if(doam.getEntity() != null && !doam.getEntity().isEmpty() && 
                doam.getEntityPath()!= null && !doam.getEntityPath().isEmpty()) {
            this.cache.cache(doam.getEntity(), doam.getEntityPath());
        }
        
        if(doam.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        doam.setOperation(routingKey);
        String msgbody = this.serializer.toJson(doam);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();

            /*
            String author = extractClientUserIdFromAuthor(doam.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }
            
            String pathowner = extractClientUserIdFromPath(doam.getEntityPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }

            String[] users = extractClientUserIdsFromReaders(doam.getUser());
            if(users != null) {
                for(String user : users) {
                    if(user != null && !user.equals(author) && !user.equals(pathowner)) {
                        List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(user, msgbody);
                        msg.addRecipient(clients);
                    }
                }
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, doam.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }
    
    private Message process_dataobject_metadata_add(String routingKey, String message) throws IOException {
        DataObjectMetadataAdd doma = (DataObjectMetadataAdd) this.serializer.fromJson(message, DataObjectMetadataAdd.class);
        
        String entityPath = convertUUIDToPathForDataObject(doma.getEntity());
        doma.setEntityPath(entityPath);
        
        // cache uuid-path
        if(doma.getEntity() != null && !doma.getEntity().isEmpty() && 
                doma.getEntityPath()!= null && !doma.getEntityPath().isEmpty()) {
            this.cache.cache(doma.getEntity(), doma.getEntityPath());
        }
        
        if(doma.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        doma.setOperation(routingKey);
        String msgbody = this.serializer.toJson(doma);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(doma.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }
            
            String pathowner = extractClientUserIdFromPath(doma.getEntityPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, doma.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }
    
    private Message process_dataobject_metadata_mod(String routingKey, String message) throws IOException {
        DataObjectMetadataMod domm = (DataObjectMetadataMod) this.serializer.fromJson(message, DataObjectMetadataMod.class);
        
        String entityPath = convertUUIDToPathForDataObject(domm.getEntity());
        domm.setEntityPath(entityPath);
        
        // cache uuid-path
        if(domm.getEntity() != null && !domm.getEntity().isEmpty() && 
                domm.getEntityPath()!= null && !domm.getEntityPath().isEmpty()) {
            this.cache.cache(domm.getEntity(), domm.getEntityPath());
        }
        
        if(domm.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        domm.setOperation(routingKey);
        String msgbody = this.serializer.toJson(domm);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            /*
            String author = extractClientUserIdFromAuthor(domm.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }
            
            String pathowner = extractClientUserIdFromPath(domm.getEntityPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }
            */
            List<Client> acceptedClients = listAcceptedClientsForDataObject(msgbody, domm.getEntityPath());
            msg.addRecipient(acceptedClients);

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private String convertUUIDToPathForDataObject(String entity) throws IOException {
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
    
    private List<Client> listAcceptedClientsForDataObject(String msgbody, String path) throws IOException {
        
        List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(msgbody);
        Map<String, Boolean> userAcceptance = new HashMap<String, Boolean>();
        
        List<Client> acceptedClients = new ArrayList<Client>();
        
        for(Client client : clients) {
            Boolean baccept = userAcceptance.get(client.getUserId());
            if(baccept == null) {
                try {
                    DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
                    if(datastoreClientInstance.hasAccessPermissionsForDataObject(path, client.getUserId())) {
                        userAcceptance.put(client.getUserId(), true);
                        acceptedClients.add(client);
                    } else {
                        userAcceptance.put(client.getUserId(), false);
                    }
                } catch (IOException ex) {
                    DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
                    if(datastoreClientInstance.hasAccessPermissionsForDataObject(path, client.getUserId())) {
                        userAcceptance.put(client.getUserId(), true);
                        acceptedClients.add(client);
                    } else {
                        userAcceptance.put(client.getUserId(), false);
                    }
                }
            } else {
                if(baccept.booleanValue()) {
                    acceptedClients.add(client);
                }
            }
        }
        
        return acceptedClients;
    }
    
    private List<Client> listAcceptedClientsForCollection(String msgbody, String path) throws IOException {
        
        List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(msgbody);
        Map<String, Boolean> userAcceptance = new HashMap<String, Boolean>();
        
        List<Client> acceptedClients = new ArrayList<Client>();
        
        for(Client client : clients) {
            Boolean baccept = userAcceptance.get(client.getUserId());
            if(baccept == null) {
                try {
                    DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
                    if(datastoreClientInstance.hasAccessPermissionsForCollection(path, client.getUserId())) {
                        userAcceptance.put(client.getUserId(), true);
                        acceptedClients.add(client);
                    } else {
                        userAcceptance.put(client.getUserId(), false);
                    }
                } catch (IOException ex) {
                    DataStoreClient datastoreClientInstance = this.datastoreClientManager.getDatastoreClientInstance();
                    if(datastoreClientInstance.hasAccessPermissionsForCollection(path, client.getUserId())) {
                        userAcceptance.put(client.getUserId(), true);
                        acceptedClients.add(client);
                    } else {
                        userAcceptance.put(client.getUserId(), false);
                    }
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
    }
}
