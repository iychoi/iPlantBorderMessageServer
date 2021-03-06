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
public class CollectionAclMod extends ADataStoreMessage {

    private boolean recursive;
    private String permission;
    private User user;
    private boolean inherit;
    
    public CollectionAclMod() {
        
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
    public User getUser() {
        return user;
    }

    @JsonProperty("user")
    public void setUser(User user) {
        this.user = user;
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
