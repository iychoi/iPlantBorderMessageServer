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

import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class CollectionAclMod {

    private User author;
    private String entity;
    private String path;
    private boolean recursive;
    private String permission;
    private List<User> user = new ArrayList<User>();
    private boolean inherit;
    
    public CollectionAclMod() {
        
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

    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    @JsonProperty("path")
    public void setPath(String path) {
        this.path = path;
    }   
    
    @JsonProperty("recursive")
    public boolean isRecursive() {
        return recursive;
    }

    @JsonProperty("recursive")
    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    @JsonProperty("permission")
    public String getPermission() {
        return permission;
    }

    @JsonProperty("permission")
    public void setPermission(String permission) {
        this.permission = permission;
    }

    @JsonProperty("user")
    public List<User> getUser() {
        return user;
    }

    @JsonProperty("user")
    public void addUser(List<User> user) {
        this.user.addAll(user);
    }
    
    @JsonProperty("user")
    public void addUser(User user) {
        this.user.add(user);
    }
    
    @JsonProperty("user")
    public void addUser(User[] user) {
        for(User u : user) {
            this.user.add(u);
        }
    }

    @JsonProperty("inherit")
    public boolean isInherit() {
        return inherit;
    }

    @JsonProperty("inherit")
    public void setInherit(boolean inherit) {
        this.inherit = inherit;
    }
}
