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
import org.iplantcollaborative.conf.MessageServerConf;

/**
 *
 * @author iychoi
 */
public class IPlantBorderMessageServer implements Closeable {

    private static final Log LOG = LogFactory.getLog(IPlantBorderMessageServer.class);

    private MessageServerConf conf;
    private Binder binder;
    private MessageProcessor processor;
    private MessagePublisher publisher;
    private MessageSubscriber subscriber;
    
    public IPlantBorderMessageServer(MessageServerConf conf) {
        this.conf = conf;
        
        Binder binder = new Binder();
        MessageProcessor processor = new MessageProcessor(binder);
        MessagePublisher publisher = new MessagePublisher(conf, binder);
        MessageSubscriber subscriber = new MessageSubscriber(conf, binder);
    }
    
    public void connect() throws IOException {
        try {
            this.publisher.connect();
            this.subscriber.connect();
        } catch (TimeoutException ex) {
            LOG.error(ex);
        }
    }
    
    @Override
    public void close() throws IOException {
        this.subscriber.close();
        this.publisher.close();
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        if(args.length == 0) {
            System.err.println("configuration file must be given");
            return;
        }
        
        String msgconf = args[0];
        
        MessageServerConf conf = MessageServerConf.createInstance(new File(msgconf));
        IPlantBorderMessageServer server = new IPlantBorderMessageServer(conf);
        server.connect();
    }
}
