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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.MessageServerConf;

/**
 *
 * @author iychoi
 */
public class MessageSubscriber implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(MessageSubscriber.class);
    
    private static final String EXCHANGE_NAME = "irods";
    
    private MessageServerConf serverConf;
    private Binder binder;
    private Connection connection;
    private Channel channel;
    private String queueName;
    private Consumer consumer;
    private Thread workerThread;
    
    public MessageSubscriber(MessageServerConf serverConf, Binder binder) {
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
        
        binder.setSubscriber(this);
    }
    
    public void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(this.serverConf.getHostname());
        factory.setPort(this.serverConf.getPort());
        factory.setUsername(this.serverConf.getUserId());
        factory.setPassword(this.serverConf.getUserPwd());
        factory.setVirtualHost(this.serverConf.getVhostSubscribe());
        
        factory.setAutomaticRecoveryEnabled(true);
        
        this.connection = factory.newConnection();
        this.channel = this.connection.createChannel();
        
        LOG.info("subscriber connected - " + this.serverConf.getHostname() + ":" + this.serverConf.getPort());
        
        this.queueName = this.channel.queueDeclare().getQueue();
        this.channel.queueBind(this.queueName, EXCHANGE_NAME, "#");
        
        this.consumer = new DefaultConsumer(this.channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                
                LOG.debug("subscribe - " + envelope.getRoutingKey() + ":" + message);
                
                MessageProcessor processor = binder.getProcessor();
                if(processor != null) {
                    processor.process(envelope.getRoutingKey(), message);
                } else {
                    LOG.error("processor not registered");
                }
                
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        
        this.workerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    channel.basicConsume(queueName, consumer);
                    LOG.info("Waiting for messages");
                } catch (IOException ex) {
                    LOG.error("Exception occurred while consuming a message", ex);
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
}
