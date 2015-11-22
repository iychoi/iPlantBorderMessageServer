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
public class CollectionMetadataAdd extends ADataStoreMessage {

    private MetaDatum metadatum;
    
    public CollectionMetadataAdd() {
        
    }
    
    @JsonProperty("metadatum")
    public MetaDatum getMetadatum() {
        return metadatum;
    }

    @JsonProperty("metadatum")
    public void setMetadatum(MetaDatum metadatum) {
        this.metadatum = metadatum;
    }   
}
