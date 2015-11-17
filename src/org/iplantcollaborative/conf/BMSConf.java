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
public class BMSConf {
    private MessageServerConf msgsvrConf;
    private DataStoreConf datastoreConf;
    
    public static BMSConf createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (BMSConf) serializer.fromJsonFile(file, BMSConf.class);
    }
    
    public static BMSConf createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (BMSConf) serializer.fromJson(json, BMSConf.class);
    }
    
    public BMSConf() {
        
    }
    
    @JsonProperty("message_server_conf")
    public MessageServerConf getMessageServerConf() {
        return msgsvrConf;
    }

    @JsonProperty("message_server_conf")
    public void setMessageServerConf(MessageServerConf msgsvrConf) {
        this.msgsvrConf = msgsvrConf;
    }
    
    @JsonProperty("datastore_conf")
    public DataStoreConf getDataStoreConfConf() {
        return datastoreConf;
    }

    @JsonProperty("datastore_conf")
    public void setDataStoreConfConf(DataStoreConf datastoreConf) {
        this.datastoreConf = datastoreConf;
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
