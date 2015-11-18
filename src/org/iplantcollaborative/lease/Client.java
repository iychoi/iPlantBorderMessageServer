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

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Client {
    // use default exchange - ""
    private static final String EXCHANGE_NAME = "";
    
    private String userId;
    private String applicationName;

    public Client() {
        
    }
    
    @JsonProperty("user_id")
    public String getUserId() {
        return userId;
    }

    @JsonProperty("user_id")
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    @JsonProperty("application_name")
    public String getApplicationName() {
        return applicationName;
    }
    
    @JsonProperty("application_name")
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }
    
    @JsonIgnore
    public String getExchange() {
        return EXCHANGE_NAME;
    }
    
    @JsonIgnore
    public String getRoutingKey() {
        return userId + "_" + applicationName;
    }
    
    @JsonIgnore
    @Override
    public int hashCode() {
        return this.userId.hashCode() ^ this.applicationName.hashCode();
    }

    @JsonIgnore
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Client other = (Client) obj;
        return this.userId.equals(other.userId) && this.applicationName.equalsIgnoreCase(other.applicationName);
    }
    
    @JsonIgnore
    @Override
    public String toString() {
        return this.userId + "_" + this.applicationName;
    }
}
