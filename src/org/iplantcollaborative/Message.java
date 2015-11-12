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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.iplantcollaborative.lease.Client;

/**
 *
 * @author iychoi
 */
public class Message {

    private Set<Client> recipients = new HashSet<Client>();
    private String messageBody;
    
    public Set<Client> getRecipients() {
        return recipients;
    }

    public void addRecipient(Client recipient) {
        if(recipient != null && !this.recipients.contains(recipient)) {
            this.recipients.add(recipient);
        }
    }
    
    public void addRecipient(Client... recipients) {
        for(Client client : recipients) {
            if(client != null) {
                if(!this.recipients.contains(client)) {
                    this.recipients.add(client);
                }
            }
        }
    }
    
    public void addRecipient(List<Client> recipients) {
        for(Client client : recipients) {
            if(client != null) {
                if(!this.recipients.contains(client)) {
                    this.recipients.add(client);
                }
            }
        }
    }

    public String getMessageBody() {
        return messageBody;
    }

    public void setMessageBody(String messageBody) {
        this.messageBody = messageBody;
    }

    public boolean hasRecipient(Client recipient) {
        return this.recipients.contains(recipient);
    }
}
