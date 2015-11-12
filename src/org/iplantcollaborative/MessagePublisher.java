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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.MessageServerConf;
import org.iplantcollaborative.lease.Client;

/**
 *
 * @author iychoi
 */
public class MessagePublisher implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(MessagePublisher.class);
    
    private MessageServerConf serverConf;
    private Binder binder;
    private Connection connection;
    private Channel channel;
    
    public MessagePublisher(MessageServerConf serverConf, Binder binder) {
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
        
        binder.setPublisher(this);
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
        
        LOG.info("publisher connected - " + this.serverConf.getHostname() + ":" + this.serverConf.getPort());
    }
    
    public void publish(List<Message> msg) throws IOException {
        for(Message m : msg) {
            publish(m);
        }
    }
    
    public void publish(Message msg) throws IOException {
        if(!this.channel.isOpen()) {
            throw new IllegalStateException("publisher is not connected");
        }
        
        for(Client c : msg.getRecipients()) {
            LOG.debug("publish - " + c.getRoutingKey() + "\t" + msg.getMessageBody());
            this.channel.basicPublish(c.getExchange(), c.getRoutingKey(), null, msg.getMessageBody().getBytes());
        }
    }

    @Override
    public void close() throws IOException {
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
}
