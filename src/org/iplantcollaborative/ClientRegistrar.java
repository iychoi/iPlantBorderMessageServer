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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.MessageServerConf;
import org.iplantcollaborative.lease.Client;
import org.iplantcollaborative.lease.Lease;
import org.iplantcollaborative.lease.msg.ARequest;
import org.iplantcollaborative.lease.msg.RequestFactory;
import org.iplantcollaborative.lease.msg.RequestLease;
import org.iplantcollaborative.lease.msg.ResponseLease;
import org.iplantcollaborative.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class ClientRegistrar implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(ClientRegistrar.class);
    
    private static final String EXCHANGE_NAME = "bms_registrations";
    private static final String QUEUE_NAME = "bms_registrations";
    private static final long DEFAULT_TIMEOUT_MIN = 10;
    
    private MessageServerConf serverConf;
    private Binder binder;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;
    private Thread workerThread;
    private JsonSerializer serializer;
    private PassiveExpiringMap<Client, Lease> leases = new PassiveExpiringMap<Client, Lease>(DEFAULT_TIMEOUT_MIN, TimeUnit.MINUTES);
    private Map<String, Set<Client>> clientMap = new HashMap<String, Set<Client>>();
    
    public ClientRegistrar(MessageServerConf serverConf, Binder binder) {
        if(serverConf == null) {
            throw new IllegalArgumentException("serverConf is null");
        }
        
        if(binder == null) {
            throw new IllegalArgumentException("binder is null");
        }
        
        initialize(serverConf, binder);
    }
    
    private void initialize(MessageServerConf serverConf, Binder binder) {
        this.serverConf = serverConf;
        this.binder = binder;
        this.serializer = new JsonSerializer();
        
        binder.setClientRegistrar(this);
    }
    
    public void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(this.serverConf.getHostname());
        factory.setPort(this.serverConf.getPort());
        factory.setUsername(this.serverConf.getUserId());
        factory.setPassword(this.serverConf.getUserPwd());
        factory.setVirtualHost(this.serverConf.getVhostPublish());
        
        factory.setAutomaticRecoveryEnabled(true);
        
        this.connection = factory.newConnection();
        this.channel = this.connection.createChannel();
        
        LOG.info("client registrar connected - " + this.serverConf.getHostname() + ":" + this.serverConf.getPort());
        
        this.channel.basicQos(1);
        //this.channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "#");
        
        this.consumer = new DefaultConsumer(this.channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                
                LOG.debug("registration - " + message);
                
                BasicProperties replyProps = new BasicProperties.Builder().correlationId(properties.getCorrelationId()).build();
                
                ARequest request = RequestFactory.getRequestInstance(message);

                // handle lease request
                if(request instanceof RequestLease) {
                    ResponseLease res = lease((RequestLease)request);
                    String response_json = serializer.toJson(res);

                    channel.basicPublish("", properties.getReplyTo(), replyProps, response_json.getBytes());
                }
                
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        
        this.workerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    channel.basicConsume(QUEUE_NAME, false, consumer);
                    LOG.info("Waiting for registrations");
                } catch (IOException ex) {
                    LOG.error("Exception occurred while consuming message", ex);
                }
            } 
        });
        this.workerThread.start();
    }

    @Override
    public void close() throws IOException {
        if(this.workerThread != null) {
            this.workerThread = null;
        }
        
        if(this.channel != null) {
            if(this.channel.isOpen()) {
                try {
                    this.channel.close();
                } catch (TimeoutException ex) {
                }
            }
            this.channel = null;
        }
        
        if(this.connection != null) {
            if(this.connection.isOpen()) {
                this.connection.close();
            }
            this.connection = null;
        }
        
        this.leases.clear();
        this.clientMap.clear();
    }
    
    public synchronized ResponseLease lease(RequestLease request) {
        Lease lease = new Lease(request);
        Client client = lease.getClient();
        
        LOG.info("registering a client - " + client.toString());
        
        this.leases.put(client, lease);
        
        Set<Client> clients = this.clientMap.get(client.getUserId());
        if(clients == null) {
            clients = new HashSet<Client>();
            this.clientMap.put(client.getUserId(), clients);
        }
        
        if(!clients.contains(client)) {
            clients.add(client);
        }
        
        ResponseLease response = new ResponseLease(lease, DEFAULT_TIMEOUT_MIN * 60 * 1000);
        return response;
    }
    
    public synchronized List<Client> getAcceptClients(String clientUserId, String msgbody) {
        List<Client> acceptedClients = new ArrayList<Client>();
        List<Client> toberemoved = new ArrayList<Client>();
        Set<Client> clients = this.clientMap.get(clientUserId);
        if(clients != null) {
            for(Client client : clients) {
                Lease lease = this.leases.get(client);
                if(lease != null) {
                    if(lease.accept(msgbody)) {
                        acceptedClients.add(client);
                    }
                } else {
                    toberemoved.add(client);
                }
            }
            
            clients.removeAll(toberemoved);
        }
        
        return acceptedClients;
    }

    public synchronized boolean accept(Client client, String msgbody) {
        Lease lease = this.leases.get(client);
        if(lease != null) {
            return lease.accept(msgbody);
        }
        return false;
    }
    
    public synchronized boolean accept(String msgbody) {
        MapIterator<Client, Lease> mapIterator = this.leases.mapIterator();
        while(mapIterator.hasNext()) {
            mapIterator.next();
            Lease lease = mapIterator.getValue();
            if(lease.accept(msgbody)) {
                return true;
            }
        }
        return false;
    }
}
