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
package org.iplantcollaborative.utils;

import java.util.List;
import org.iplantcollaborative.datastore.msg.User;

/**
 *
 * @author iychoi
 */
public class PathUtils {
    
    public static String getParentPath(String path) {
        if(path == null || path.isEmpty() || path.equals("/")) {
            return null;
        }
        
        String parentPath = path;
        if(parentPath.endsWith("/")) {
            parentPath = parentPath.substring(0, parentPath.length()-1);
        }
        
        int lastIndexOf = parentPath.lastIndexOf("/");
        if(lastIndexOf >= 0) {
            parentPath = parentPath.substring(0, lastIndexOf);
            return parentPath;
        }
        return null;
    }
    
    public static String extractUserNameFromPath(String path) {
        if(path == null || path.isEmpty()) {
            return null;
        }
        
        int idx = path.indexOf("/home/");
        if(idx >= 0) {
            String tail = path.substring(idx + 6);
            int end = tail.indexOf("/");
            if(end >= 0) {
                tail = tail.substring(0, end);
            }
            return tail;
        }
        return null;
    }
    
    public static String extractClientUserIdFromAuthor(User author) {
        return author.getName();
    }
    
    public static String extractClientUserIdFromPath(String path) {
        return extractUserNameFromPath(path);
    }

    public static String[] extractClientUserIdsFromReaders(User reader) {
        String[] userIds = new String[1];
        
        String userId = extractClientUserIdFromAuthor(reader);
        userIds[0] = userId;
        
        return userIds;
    }
    
    public static String[] extractClientUserIdsFromReaders(List<User> readers) {
        String[] userIds = new String[readers.size()];
        
        int i=0;
        for(User reader : readers) {
            String userId = extractClientUserIdFromAuthor(reader);
            userIds[i] = userId;
            i++;
        }
        
        return userIds;
    }
}
