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
package org.iplantcollaborative.datastore.msg;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class CollectionMv {

    private User author;
    private String entity;
    private String old_path;
    private String new_path;
    
    public CollectionMv() {
        
    }
    
    @JsonProperty("author")
    public User getAuthor() {
        return author;
    }

    @JsonProperty("author")
    public void setAuthor(User author) {
        this.author = author;
    }

    @JsonProperty("entity")
    public String getEntity() {
        return entity;
    }

    @JsonProperty("entity")
    public void setEntity(String entity) {
        this.entity = entity;
    }

    @JsonProperty("old-path")
    public String getOldPath() {
        return old_path;
    }

    @JsonProperty("old-path")
    public void setOldPath(String old_path) {
        this.old_path = old_path;
    }
    
    @JsonProperty("new-path")
    public String getNewPath() {
        return new_path;
    }

    @JsonProperty("new-path")
    public void setNewPath(String new_path) {
        this.new_path = new_path;
    }
}
