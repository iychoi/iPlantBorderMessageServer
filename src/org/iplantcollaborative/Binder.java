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

/**
 *
 * @author iychoi
 */
public class Binder {

    private MessagePublisher publisher;
    private DataStoreMessageReceiver receiver;
    private DataStoreMessageProcessor processor;
    private ClientRegistrar clientRegistrar;
    
    public Binder() {
        
    }
    
    public MessagePublisher getPublisher() {
        return publisher;
    }

    public void setPublisher(MessagePublisher publisher) {
        this.publisher = publisher;
    }

    public DataStoreMessageReceiver getReceiver() {
        return receiver;
    }

    public void setReceiver(DataStoreMessageReceiver receiver) {
        this.receiver = receiver;
    }

    public DataStoreMessageProcessor getProcessor() {
        return processor;
    }
    
    public void setProcessor(DataStoreMessageProcessor processor) {
        this.processor = processor;
    }

    public ClientRegistrar getClientRegistrar() {
        return clientRegistrar;
    }
    
    public void setClientRegistrar(ClientRegistrar clientRegistrar) {
        this.clientRegistrar = clientRegistrar;
    }
}
