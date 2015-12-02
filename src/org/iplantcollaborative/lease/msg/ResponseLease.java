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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.iplantcollaborative.lease.Lease;

/**
 *
 * @author iychoi
 */
public class ResponseLease extends AResponse {

    private Date leaseStart;
    private Date leaseExpire;
    
    public ResponseLease() {
        
    }

    public ResponseLease(Lease lease, long leaseTerm) {
        this.client = lease.getClient();
        this.leaseStart = lease.getLeaseTime();
        this.leaseExpire = new Date(this.leaseStart.getTime() + leaseTerm);
    }
    
    @JsonProperty("lease_start")
    public Date getLeaseStart() {
        return leaseStart;
    }

    @JsonProperty("lease_start")
    public void setLeaseStart(Date leaseStart) {
        this.leaseStart = leaseStart;
    }
    
    @JsonProperty("lease_expire")
    public Date getLeaseExpire() {    
        return leaseExpire;
    }
    
    @JsonProperty("lease_expire")
    public void setLeaseExpire(Date leaseExpire) {
        this.leaseExpire = leaseExpire;
    }

    @JsonIgnore
    public String getRoutingKey() {
        return "lease";
    }
}
