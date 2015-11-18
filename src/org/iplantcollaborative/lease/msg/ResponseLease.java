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
package org.iplantcollaborative.lease.msg;

import java.util.Date;
import org.codehaus.jackson.annotate.JsonProperty;
import org.iplantcollaborative.lease.Client;
import org.iplantcollaborative.lease.Lease;

/**
 *
 * @author iychoi
 */
public class ResponseLease {

    private Client client;
    private Date leaseStart;
    
    public ResponseLease() {
        
    }

    public ResponseLease(Lease lease) {
        this.client = lease.getUser();
        this.leaseStart = lease.getLeaseTime();
    }
    
    @JsonProperty("client")
    public Client getClient() {
        return client;
    }

    @JsonProperty("client")
    public void setClient(Client client) {
        this.client = client;
    }

    @JsonProperty("lease_start")
    public Date getLeaseStart() {
        return leaseStart;
    }

    @JsonProperty("lease_start")
    public void setLeaseStart(Date leaseStart) {
        this.leaseStart = leaseStart;
    }
}
