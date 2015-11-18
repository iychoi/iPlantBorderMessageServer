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

import java.io.IOException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.iplantcollaborative.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class RequestFactory {
    public static ARequest getRequestInstance(String jsonRequest) {
        try {
            ObjectMapper m = new ObjectMapper();
            
            JsonNode rootNode = m.readTree(jsonRequest);
            
            // request
            JsonNode reqNode = rootNode.get("req");
            if(reqNode != null) {
                String reqtype = reqNode.asText();
                if(reqtype != null) {
                    return createRequestInstance(reqtype, jsonRequest);
                }
            }
            return null;
        } catch (IOException ex) {
            return null;
        }
    }

    private static ARequest createRequestInstance(String reqtype, String jsonRequest) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        
        switch(reqtype) {
            case "lease":
                return (RequestLease) serializer.fromJson(jsonRequest, RequestLease.class);
            default:
                return null;
        }
    }
}
