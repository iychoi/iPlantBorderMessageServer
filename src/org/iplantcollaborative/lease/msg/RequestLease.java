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

import org.iplantcollaborative.lease.AcceptorConfig;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class RequestLease extends ARequest {

    private List<AcceptorConfig> acceptors = new ArrayList<AcceptorConfig>();
    
    public RequestLease() {
        
    }
    
    @JsonProperty("acceptors")
    public void addAcceptor(List<AcceptorConfig> acceptorConfig) {
        this.acceptors.addAll(acceptorConfig);
    }
    
    @JsonIgnore
    public void addAcceptor(AcceptorConfig acceptorConfig) {
        this.acceptors.add(acceptorConfig);
    }
    
    @JsonProperty("acceptors")
    public List<AcceptorConfig> getAcceptor() {
        return this.acceptors;
    }
}
