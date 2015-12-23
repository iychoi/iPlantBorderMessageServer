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
package org.iplantcollaborative;

import java.util.Objects;
import org.apache.commons.collections4.map.LRUMap;

/**
 *
 * @author iychoi
 */
public class AccessPermissionCache {
    
    private static final int DEFAULT_CACHE_SIZE = 100000;
    
    private LRUMap<AccessPermissionKey, Boolean> cache = new LRUMap<AccessPermissionKey, Boolean>(DEFAULT_CACHE_SIZE);
    
    public static class AccessPermissionKey {
        private String userId;
        private String path;

        public AccessPermissionKey(String userId, String path) {
            this.userId = userId;
            this.path = path;
        }
        
        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
        
        @Override
        public int hashCode() {
            return this.userId.hashCode() ^ this.path.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final AccessPermissionKey other = (AccessPermissionKey) obj;
            if (!Objects.equals(this.userId, other.userId)) {
                return false;
            }
            if (!Objects.equals(this.path, other.path)) {
                return false;
            }
            return true;
        }
        
        @Override
        public String toString() {
            return this.userId + "@" + this.path;
        }
    }
    
    public AccessPermissionCache() {
    }
    
    public void cache(AccessPermissionKey key, Boolean allowed) {
        this.cache.put(key, allowed);
    }
    
    public Boolean get(AccessPermissionKey key) {
        return this.cache.get(key);
    }
    
    public void clearCache() {
        this.cache.clear();
    }
}
