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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.MessageServerConf;
import org.iplantcollaborative.lease.AcceptorFactory;
import org.iplantcollaborative.lease.Client;
import org.iplantcollaborative.lease.IMessageAcceptor;
import org.iplantcollaborative.lease.Lease;
import org.iplantcollaborative.lease.msg.AcceptorConfig;
import org.iplantcollaborative.lease.msg.ReqestLease;
import org.iplantcollaborative.lease.msg.ResponseLease;
import org.iplantcollaborative.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class ClientRegistrar implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(ClientRegistrar.class);
    
    private static final String EXCHANGE_NAME = "";
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
        
        this.channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        this.channel.basicQos(1);
        
        this.consumer = new QueueingConsumer(this.channel);
        this.workerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    channel.basicConsume(QUEUE_NAME, false, consumer);
                    LOG.info("Waiting for registrations");
                    while(true) {
                        QueueingConsumer qconsumer = (QueueingConsumer)consumer;
                        try {
                            QueueingConsumer.Delivery delivery = qconsumer.nextDelivery();
                            BasicProperties properties = delivery.getProperties();    
                            BasicProperties replyProps = new BasicProperties.Builder().correlationId(properties.getCorrelationId()).build();
                            
                            String message = new String(delivery.getBody(), "UTF-8");
                            
                            LOG.debug("registration - " + message);
                            
                            ReqestLease req = (ReqestLease) serializer.fromJson(message, ReqestLease.class);
                            ResponseLease res = lease(req);
                            
                            String response_json = serializer.toJson(res);
                            
                            channel.basicPublish("", properties.getReplyTo(), replyProps, response_json.getBytes());
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        } catch (InterruptedException ex) {
                            LOG.error(ex);
                            break;
                        } catch (ShutdownSignalException ex) {
                            LOG.error(ex);
                            break;
                        } catch (ConsumerCancelledException ex) {
                            LOG.error(ex);
                            break;
                        }
                    }
                } catch (IOException ex) {
                    LOG.error(ex);
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
    }
    
    public ResponseLease lease(ReqestLease request) {
        Lease lease = new Lease();
        lease.setUser(request.getClient());
        
        for(AcceptorConfig config : request.getAcceptor()) {
            IMessageAcceptor acceptorInstance = AcceptorFactory.getAcceptorInstance(config.getAcceptor(), config.getPattern());
            if(acceptorInstance != null) {
                lease.addAcceptor(acceptorInstance);
            }
        }
        
        this.leases.put(lease.getUser(), lease);
        
        ResponseLease response = new ResponseLease();
        response.setClient(request.getClient());
        response.setLeaseStart(lease.getLeaseTime());
        
        return response;
    }

    public boolean accept(Client author, String msgbody) {
        Lease lease = this.leases.get(author);
        if(lease != null) {
            return lease.accept(msgbody);
        }
        return false;
    }
}
