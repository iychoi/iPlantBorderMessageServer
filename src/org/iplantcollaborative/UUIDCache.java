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

import org.apache.commons.collections4.map.LRUMap;

/**
 *
 * @author iychoi
 */
public class UUIDCache {
    
    private static final int DEFAULT_CACHE_SIZE = 10000;
    
    private LRUMap<String, String> cache = new LRUMap<String, String>(DEFAULT_CACHE_SIZE);
    
    public UUIDCache() {
    }
    
    public void cache(String uuid, String path) {
        this.cache.put(uuid, path);
    }
    
    public String get(String uuid) {
        return this.cache.get(uuid);
    }
    
    public void clearCache() {
        this.cache.clear();
    }
}
