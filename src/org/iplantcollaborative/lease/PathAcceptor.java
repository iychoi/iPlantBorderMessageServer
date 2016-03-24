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
package org.iplantcollaborative.lease;

import org.iplantcollaborative.datastore.msg.ADataStoreMessage;
import org.iplantcollaborative.datastore.msg.CollectionAclMod;
import org.iplantcollaborative.datastore.msg.CollectionAdd;
import org.iplantcollaborative.datastore.msg.CollectionMetadataAdd;
import org.iplantcollaborative.datastore.msg.CollectionMv;
import org.iplantcollaborative.datastore.msg.CollectionRm;
import org.iplantcollaborative.datastore.msg.DataObjectAclMod;
import org.iplantcollaborative.datastore.msg.DataObjectAdd;
import org.iplantcollaborative.datastore.msg.DataObjectMetadataAdd;
import org.iplantcollaborative.datastore.msg.DataObjectMetadataMod;
import org.iplantcollaborative.datastore.msg.DataObjectMod;
import org.iplantcollaborative.datastore.msg.DataObjectMv;
import org.iplantcollaborative.datastore.msg.DataObjectRm;

/**
 *
 * @author iychoi
 */
public class PathAcceptor implements IMessageAcceptor {

    private String pattern;
    
    public PathAcceptor() {
    }
    
    private boolean wildCardMatch(String text) {
        String[] cards = this.pattern.split("\\*");
        for (String card : cards) {
            int idx = text.indexOf(card);

            if (idx == -1) {
                return false;
            }

            text = text.substring(idx + card.length());
        }
        return true;
    }

    @Override
    public boolean accept(ADataStoreMessage message) {
        if(this.pattern.equals("*")) {
            return true;
        }
        
        if(message instanceof CollectionAclMod) {
            return accept((CollectionAclMod)message);
        } else if(message instanceof CollectionAdd) {
            return accept((CollectionAdd)message);
        } else if(message instanceof CollectionMetadataAdd) {
            return accept((CollectionMetadataAdd)message);
        } else if(message instanceof CollectionMv) {
            return accept((CollectionMv)message);
        } else if(message instanceof CollectionRm) {
            return accept((CollectionRm)message);
        } else if(message instanceof DataObjectAclMod) {
            return accept((DataObjectAclMod)message);
        } else if(message instanceof DataObjectAdd) {
            return accept((DataObjectAdd)message);
        } else if(message instanceof DataObjectMetadataAdd) {
            return accept((DataObjectMetadataAdd)message);
        } else if(message instanceof DataObjectMetadataMod) {
            return accept((DataObjectMetadataMod)message);
        } else if(message instanceof DataObjectMod) {
            return accept((DataObjectMod)message);
        } else if(message instanceof DataObjectMv) {
            return accept((DataObjectMv)message);
        } else if(message instanceof DataObjectRm) {
            return accept((DataObjectRm)message);
        }
        return false;
    }
    
    public boolean accept(CollectionAclMod message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        return accept;
    }
    
    public boolean accept(CollectionAdd message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        
        if(!accept) {
            String path = message.getPath();
            if(path != null) {
                accept = accept || wildCardMatch(path);
            }
        }
        return accept;
    }
    
    public boolean accept(CollectionMetadataAdd message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        return accept;
    }
    
    public boolean accept(CollectionMv message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        
        if(!accept) {
            String oldPath = message.getOldPath();
            if(oldPath != null) {
                accept = accept || wildCardMatch(oldPath);
            }
        }
        
        if(!accept) {
            String newPath = message.getNewPath();
            if(newPath != null) {
                accept = accept || wildCardMatch(newPath);
            }
        }
        return accept;
    }
    
    public boolean accept(CollectionRm message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        
        if(!accept) {
            String path = message.getPath();
            if(path != null) {
                accept = accept || wildCardMatch(path);
            }
        }
        return accept;
    }
    
    public boolean accept(DataObjectAclMod message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        return accept;
    }
    
    public boolean accept(DataObjectAdd message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        
        if(!accept) {
            String path = message.getPath();
            if(path != null) {
                accept = accept || wildCardMatch(path);
            }
        }
        return accept;
    }
    
    public boolean accept(DataObjectMetadataAdd message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        return accept;
    }
    
    public boolean accept(DataObjectMetadataMod message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        return accept;
    }
    
    public boolean accept(DataObjectMod message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        return accept;
    }
    
    public boolean accept(DataObjectMv message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        
        if(!accept) {
            String oldPath = message.getOldPath();
            if(oldPath != null) {
                accept = accept || wildCardMatch(oldPath);
            }
        }
        
        if(!accept) {
            String newPath = message.getNewPath();
            if(newPath != null) {
                accept = accept || wildCardMatch(newPath);
            }
        }
        return accept;
    }
    
    public boolean accept(DataObjectRm message) {
        boolean accept = false;
        if(this.pattern.equals("*")) {
            return true;
        }
        
        String entityPath = message.getEntityPath();
        if(entityPath != null) {
            accept = accept || wildCardMatch(entityPath);
        }
        
        if(!accept) {
            String path = message.getPath();
            if(path != null) {
                accept = accept || wildCardMatch(path);
            }
        }
        return accept;
    }
    
    @Override
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
}
