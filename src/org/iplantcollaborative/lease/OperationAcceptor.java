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

/**
 *
 * @author iychoi
 */
public class OperationAcceptor implements IMessageAcceptor {

    private String pattern;
    
    public OperationAcceptor() {
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
        
        String operation = message.getOperation();
        if(operation != null) {
            return wildCardMatch(operation);
        }
        return false;
    }

    @Override
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
}
