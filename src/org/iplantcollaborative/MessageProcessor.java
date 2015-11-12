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

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
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
import org.iplantcollaborative.lease.Client;
import org.iplantcollaborative.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class MessageProcessor {
    
    private static final Log LOG = LogFactory.getLog(MessageProcessor.class);
    
    private Binder binder;
    private JsonSerializer serializer;
    private UUIDCache cache;
    
    public MessageProcessor(Binder binder) {
        this.binder = binder;
        
        binder.setProcessor(this);
        
        this.serializer = new JsonSerializer();
        this.cache = new UUIDCache();
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
    
    private Client extractClientFromAuthor(User author) {
        Client client = new Client();
        client.setUserId(author.getName());
        return client;
    }
    
    private Client extractClientFromPath(String path) {
        String name = extractUserNameFromPath(path);
        Client client = new Client();
        client.setUserId(name);
        return client;
    }
    
    private Client[] extractClientsFromReaders(List<User> readers) {
        Client[] clients = new Client[readers.size()];
        
        int i=0;
        for(User reader : readers) {
            Client client = extractClientFromAuthor(reader);
            clients[i] = client;
            i++;
        }
        
        return clients;
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
            
            Client author = extractClientFromAuthor(ca.getAuthor());
            if (author != null) {
                if (!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client pathowner = extractClientFromPath(ca.getPath());
            if (pathowner != null) {
                if (!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
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
            
            Client author = extractClientFromAuthor(cr.getAuthor());
            if (author != null) {
                if (!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client pathowner = extractClientFromPath(cr.getPath());
            if (pathowner != null) {
                if (!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
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
            
            Client author = extractClientFromAuthor(cm.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client pathownerOld = extractClientFromPath(cm.getOldPath());
            if(pathownerOld != null) {
                if(!msg.hasRecipient(pathownerOld)) {
                    if (this.binder.getClientRegistrar().accept(pathownerOld, msgbody)) {
                        msg.addRecipient(pathownerOld);
                    }
                }
            }

            Client pathownerNew = extractClientFromPath(cm.getNewPath());
            if(pathownerNew != null) {
                if(!msg.hasRecipient(pathownerNew)) {
                    if (this.binder.getClientRegistrar().accept(pathownerNew, msgbody)) {
                        msg.addRecipient(pathownerNew);
                    }
                }
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
            
            Client author = extractClientFromAuthor(cam.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client pathowner = extractClientFromPath(cam.getPath());
            if(pathowner != null) {
                if(!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
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
            
            Client author = extractClientFromAuthor(doa.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client creator = extractClientFromAuthor(doa.getCreator());
            if(creator != null) {
                if(!msg.hasRecipient(creator)) {
                    if (this.binder.getClientRegistrar().accept(creator, msgbody)) {
                        msg.addRecipient(creator);
                    }
                }
            }

            Client pathowner = extractClientFromPath(doa.getPath());
            if(pathowner != null) {
                if(!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
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
            
            Client author = extractClientFromAuthor(dor.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client pathowner = extractClientFromPath(dor.getPath());
            if(pathowner != null) {
                if(!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
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

            Client author = extractClientFromAuthor(dom.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client creator = extractClientFromAuthor(dom.getCreator());
            if(creator != null) {
                if(!msg.hasRecipient(creator)) {
                    if (this.binder.getClientRegistrar().accept(creator, msgbody)) {
                        msg.addRecipient(creator);
                    }
                }
            }

            Client pathowner = extractClientFromPath(dom.getPath());
            if(pathowner != null) {
                if(!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
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
            
            Client author = extractClientFromAuthor(dom.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client pathownerOld = extractClientFromPath(dom.getOldPath());
            if(pathownerOld != null) {
                if(!msg.hasRecipient(pathownerOld)) {
                    if (this.binder.getClientRegistrar().accept(pathownerOld, msgbody)) {
                        msg.addRecipient(pathownerOld);
                    }
                }
            }

            Client pathownerNew = extractClientFromPath(dom.getNewPath());
            if(pathownerNew != null) {
                if(!msg.hasRecipient(pathownerNew)) {
                    if (this.binder.getClientRegistrar().accept(pathownerNew, msgbody)) {
                        msg.addRecipient(pathownerNew);
                    }
                }
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

            Client author = extractClientFromAuthor(doam.getAuthor());
            if(author != null) {
                if(!msg.hasRecipient(author)) {
                    if (this.binder.getClientRegistrar().accept(author, msgbody)) {
                        msg.addRecipient(author);
                    }
                }
            }

            Client[] users = extractClientsFromReaders(doam.getUser());
            if(users != null) {
                for(Client user : users) {
                    if(!msg.hasRecipient(user)) {
                        if (this.binder.getClientRegistrar().accept(user, msgbody)) {
                            msg.addRecipient(user);
                        }
                    }
                }
            }

            Client pathowner = extractClientFromPath(doam.getPath());
            if(pathowner != null) {
                if(!msg.hasRecipient(pathowner)) {
                    if (this.binder.getClientRegistrar().accept(pathowner, msgbody)) {
                        msg.addRecipient(pathowner);
                    }
                }
            }

            msg.setMessageBody(msgbody);
            return msg;
        } else {
            return null;
        }
    }

    private String convertUUIDToPath(String entity) {
        //TODO: convert 
        return null;
    }    
}
