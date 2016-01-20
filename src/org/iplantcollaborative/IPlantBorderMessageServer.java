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
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.iplantcollaborative.conf.BMSConf;
import org.iplantcollaborative.conf.DataStoreConf;
import org.iplantcollaborative.conf.MessageServerConf;

/**
 *
 * @author iychoi
 */
public class IPlantBorderMessageServer implements Closeable {

    private static final Log LOG = LogFactory.getLog(IPlantBorderMessageServer.class);

    private MessageServerConf msgsvrConf;
    private DataStoreConf datastoreConf;
    private Binder binder;
    private DataStoreMessageProcessor processor;
    private MessagePublisher publisher;
    private DataStoreMessageReceiver subscriber;
    private ClientRegistrar registrar;
    
    public IPlantBorderMessageServer(MessageServerConf msgsvrConf, DataStoreConf datastoreConf) {
        this.msgsvrConf = msgsvrConf;
        this.datastoreConf = datastoreConf;
        
        this.binder = new Binder();
        this.processor = new DataStoreMessageProcessor(this.datastoreConf, this.binder);
        this.publisher = new MessagePublisher(this.msgsvrConf, this.binder);
        this.subscriber = new DataStoreMessageReceiver(this.msgsvrConf, this.binder);
        this.registrar = new ClientRegistrar(this.msgsvrConf, this.binder);
    }
    
    public void connect() throws IOException {
        try {
            this.processor.connect();
            this.publisher.connect();
            this.subscriber.connect();
            this.registrar.connect();
        } catch (TimeoutException ex) {
            LOG.error("Exception occurred while connecting", ex);
        }
    }
    
    @Override
    public void close() throws IOException {
        this.registrar.close();
        this.subscriber.close();
        this.publisher.close();
        this.processor.close();
    }
    
    public MessageServerConf getMessageServerConf() {
        return msgsvrConf;
    }

    public DataStoreConf getDataStoreConf() {
        return datastoreConf;
    }

    public DataStoreMessageProcessor getProcessor() {
        return processor;
    }

    public MessagePublisher getPublisher() {
        return publisher;
    }

    public DataStoreMessageReceiver getSubscriber() {
        return subscriber;
    }

    public ClientRegistrar getRegistrar() {
        return registrar;
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        if(args.length == 0) {
            System.err.println("configuration file must be given");
            return;
        }
        
        String bmsconf = args[0];
        
        BMSConf conf = BMSConf.createInstance(new File(bmsconf));
        IPlantBorderMessageServer server = new IPlantBorderMessageServer(conf.getMessageServerConf(), conf.getDataStoreConfConf());
        server.connect();
    }
}
