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
import java.io.StringWriter;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.iplantcollaborative.conf.DataStoreConf;
import org.iplantcollaborative.datastore.msg.CollectionAclMod;
import org.iplantcollaborative.datastore.msg.CollectionAdd;
import org.iplantcollaborative.datastore.msg.CollectionMv;
import org.iplantcollaborative.datastore.msg.CollectionRm;
import org.iplantcollaborative.datastore.msg.DataObjectAclMod;
import org.iplantcollaborative.datastore.msg.DataObjectAdd;
import org.iplantcollaborative.datastore.msg.DataObjectMod;
import org.iplantcollaborative.datastore.msg.DataObjectMv;
import org.iplantcollaborative.datastore.msg.DataObjectRm;
import org.iplantcollaborative.datastore.msg.User;
import org.iplantcollaborative.irods.DataStoreClient;
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
    private DataStoreClient datastoreClient;
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
        
        this.datastoreClient = new DataStoreClient(datastoreConf);
    }
    
    public void connect() throws IOException {
        this.datastoreClient.connect();
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
                default:
                    LOG.info("message has no processor - ignored - " + routingKey);
                    break;
            }

            if(msg != null) {
                MessagePublisher publisher = this.binder.getPublisher();
                if(publisher != null) {
                    try {
                        publisher.publish(msg);
                    } catch (IOException ex) {
                        LOG.error("publish failed " + ex.toString());
                    }
                } else {
                    LOG.error("processor not registered");
                }
            }
        } catch (Exception ex) {
            LOG.error("process failed " + ex.toString());
        }
    }

    private String addOperation(String json, String operation) throws IOException {
        StringWriter writer = new StringWriter();
        ObjectMapper m = new ObjectMapper();
        
        JsonNode rootNode = m.readTree(json);
        ((ObjectNode)rootNode).put("operation", operation);
        
        m.writeValue(writer, rootNode);
        return writer.getBuffer().toString();
    }
    
    private String extractUserNameFromPath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
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
    
    private Message process_collection_add(String routingKey, String message) throws IOException {
        CollectionAdd ca = (CollectionAdd) this.serializer.fromJson(message, CollectionAdd.class);
        
        // cache uuid-path
        if(ca.getEntity() != null && !ca.getEntity().isEmpty() && 
                ca.getPath() != null && !ca.getPath().isEmpty()) {
            this.cache.cache(ca.getEntity(), ca.getPath());
        }
        
        if(ca.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_collection_rm(String routingKey, String message) throws IOException {
        CollectionRm cr = (CollectionRm) this.serializer.fromJson(message, CollectionRm.class);
        
        // cache uuid-path
        if(cr.getEntity() != null && !cr.getEntity().isEmpty() && 
                cr.getPath() != null && !cr.getPath().isEmpty()) {
            this.cache.cache(cr.getEntity(), cr.getPath());
        }
        
        if(cr.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_collection_mv(String routingKey, String message) throws IOException {
        CollectionMv cm = (CollectionMv) this.serializer.fromJson(message, CollectionMv.class);
        
        // cache uuid-path
        if(cm.getEntity() != null && !cm.getEntity().isEmpty() && 
                cm.getNewPath() != null && !cm.getNewPath().isEmpty()) {
            this.cache.cache(cm.getEntity(), cm.getNewPath());
        }
        
        if(cm.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_collection_acl_mod(String routingKey, String message) throws IOException {
        CollectionAclMod cam = (CollectionAclMod) this.serializer.fromJson(message, CollectionAclMod.class);
        
        // cache uuid-path
        if(cam.getEntity() != null && !cam.getEntity().isEmpty() && 
                cam.getPath()!= null && !cam.getPath().isEmpty()) {
            this.cache.cache(cam.getEntity(), cam.getPath());
        }
        
        if(cam.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
            String author = extractClientUserIdFromAuthor(cam.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }

            String pathowner = extractClientUserIdFromPath(cam.getPath());
            if(pathowner != null && !pathowner.equals(author)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_add(String routingKey, String message) throws IOException {
        DataObjectAdd doa = (DataObjectAdd) this.serializer.fromJson(message, DataObjectAdd.class);
        
        // cache uuid-path
        if(doa.getEntity() != null && !doa.getEntity().isEmpty() && 
                doa.getPath()!= null && !doa.getPath().isEmpty()) {
            this.cache.cache(doa.getEntity(), doa.getPath());
        }
        
        if(doa.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_rm(String routingKey, String message) throws IOException {
        DataObjectRm dor = (DataObjectRm) this.serializer.fromJson(message, DataObjectRm.class);
        
        // cache uuid-path
        if(dor.getEntity() != null && !dor.getEntity().isEmpty() && 
                dor.getPath()!= null && !dor.getPath().isEmpty()) {
            this.cache.cache(dor.getEntity(), dor.getPath());
        }
        
        if(dor.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_mod(String routingKey, String message) throws IOException {
        DataObjectMod dom = (DataObjectMod) this.serializer.fromJson(message, DataObjectMod.class);
        
        if(dom.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String path = convertUUIDToPath(dom.getEntity());
        dom.setPath(path);
        
        // cache uuid-path
        if(dom.getEntity() != null && !dom.getEntity().isEmpty() && 
                dom.getPath()!= null && !dom.getPath().isEmpty()) {
            this.cache.cache(dom.getEntity(), dom.getPath());
        }
        
        String msgbody = addOperation(this.serializer.toJson(dom), routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();

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

            String pathowner = extractClientUserIdFromPath(dom.getPath());
            if(pathowner != null && !pathowner.equals(author) && !pathowner.equals(creator)) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(pathowner, msgbody);
                msg.addRecipient(clients);
            }

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_mv(String routingKey, String message) throws IOException {
        DataObjectMv dom = (DataObjectMv) this.serializer.fromJson(message, DataObjectMv.class);
        
        // cache uuid-path
        if(dom.getEntity() != null && !dom.getEntity().isEmpty() && 
                dom.getNewPath()!= null && !dom.getNewPath().isEmpty()) {
            this.cache.cache(dom.getEntity(), dom.getNewPath());
        }
        
        if(dom.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String msgbody = addOperation(message, routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();
            
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private Message process_dataobject_acl_mod(String routingKey, String message) throws IOException {
        DataObjectAclMod doam = (DataObjectAclMod) this.serializer.fromJson(message, DataObjectAclMod.class);
        
        if(doam.getAuthor() == null) {
            throw new IOException("message has no author field");
        }
        
        String path = convertUUIDToPath(doam.getEntity());
        doam.setPath(path);
        
        // cache uuid-path
        if(doam.getEntity() != null && !doam.getEntity().isEmpty() && 
                doam.getPath()!= null && !doam.getPath().isEmpty()) {
            this.cache.cache(doam.getEntity(), doam.getPath());
        }
        
        String msgbody = addOperation(this.serializer.toJson(doam), routingKey);
        
        if(this.binder.getClientRegistrar() != null) {
            Message msg = new Message();

            String author = extractClientUserIdFromAuthor(doam.getAuthor());
            if(author != null) {
                List<Client> clients = this.binder.getClientRegistrar().getAcceptClients(author, msgbody);
                msg.addRecipient(clients);
            }
            
            String pathowner = extractClientUserIdFromPath(doam.getPath());
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

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private String convertUUIDToPath(String entity) throws IOException {
        return this.datastoreClient.convertUUIDToPath(entity);
    }
    
    public DataStoreClient getDatastoreClient() {
        return datastoreClient;
    }

    @Override
    public void close() throws IOException {
        this.datastoreClient.close();
        this.datastoreClient = null;
    }
}
