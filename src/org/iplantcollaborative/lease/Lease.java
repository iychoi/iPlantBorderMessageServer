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
package org.iplantcollaborative.lease;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class Lease {
    private Client user;
    private List<IMessageAcceptor> acceptors = new ArrayList<IMessageAcceptor>();
    private Date leaseTime;

    public Lease() {
        this.leaseTime = new Date();
    }
    
    public Client getUser() {
        return user;
    }

    public void setUser(Client user) {
        this.user = user;
    }

    public List<IMessageAcceptor> getAcceptors() {
        return acceptors;
    }

    public void addAcceptors(List<IMessageAcceptor> acceptors) {
        this.acceptors.addAll(acceptors);
    }
    
    public void addAcceptor(IMessageAcceptor... acceptors) {
        for(IMessageAcceptor acceptor : acceptors) {
            this.acceptors.add(acceptor);
        }
    }
    
    public boolean accept(String message) {
        if(this.acceptors.size() == 0) {
            return false;
        }
        
        for(IMessageAcceptor acceptor : this.acceptors) {
            if(!acceptor.accept(message)) {
                return false;
            }
        }
        return true;
    }
    
    public Date getLeaseTime() {
        return leaseTime;
    }
}
