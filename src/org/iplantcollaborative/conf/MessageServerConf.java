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
package org.iplantcollaborative.conf;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.iplantcollaborative.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class MessageServerConf {

    private String hostname;
    private int port;
    private String userId;
    private String userPwd;
    private String vhostSubscribe;
    private String vhostPublish;

    public static MessageServerConf createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (MessageServerConf) serializer.fromJsonFile(file, MessageServerConf.class);
    }
    
    public static MessageServerConf createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (MessageServerConf) serializer.fromJson(json, MessageServerConf.class);
    }
    
    public MessageServerConf() {
        
    }
    
    @JsonProperty("hostname")
    public String getHostname() {
        return hostname;
    }

    @JsonProperty("hostname")
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @JsonProperty("port")
    public int getPort() {
        return port;
    }

    @JsonProperty("port")
    public void setPort(int port) {
        this.port = port;
    }

    @JsonProperty("user_id")
    public String getUserId() {
        return userId;
    }

    @JsonProperty("user_id")
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @JsonProperty("user_pwd")
    public String getUserPwd() {
        return userPwd;
    }

    @JsonProperty("user_pwd")
    public void setUserPwd(String userPwd) {
        this.userPwd = userPwd;
    }

    @JsonProperty("vhost_subscribe")
    public String getVhostSubscribe() {
        return vhostSubscribe;
    }

    @JsonProperty("vhost_subscribe")
    public void setVhostSubscribe(String vhostSubscribe) {
        this.vhostSubscribe = vhostSubscribe;
    }

    @JsonProperty("vhost_publish")
    public String getVhostPublish() {
        return vhostPublish;
    }

    @JsonProperty("vhost_publish")
    public void setVhostPublish(String vhostPublish) {
        this.vhostPublish = vhostPublish;
    }
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
}
