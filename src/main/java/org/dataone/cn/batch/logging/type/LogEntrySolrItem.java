/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.batch.logging.type;

import java.io.Serializable;
import org.apache.solr.client.solrj.beans.Field;
import org.dataone.service.types.v1.LogEntry;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
/**
 *  Allows the LogEntry domain object to be mapped to a Solr POJO
 * 
 * @author waltz
 */
public class LogEntrySolrItem implements Serializable {

    @Field("id")
    String id;

    @Field("dateAggregated")
    Date dateAggregated;

    @Field("entryId")
    String entryId;

    @Field("isPublic")
    boolean isPublic = false;

    // really the list of subjects that have read Permission
    @Field("readPermission")
    List<String> readPermission;

    @Field("pid")
    String pid;

    @Field("ipAddress")
    String ipAddress;

    @Field("userAgent")
    String userAgent;

    @Field("subject")
    String subject;

    @Field("event")
    String event;

    @Field("dateLogged")
    Date dateLogged;

    @Field("nodeId")
    String nodeIdentifier;
    
    @Field("formatId")
    String formatId;
    
    @Field("size")
    BigInteger size;
    
    @Field("rightsholder")
    String rightsHolder;
    
    @Field("country")
    String country;
    
    @Field("region")
    String region;
    
    @Field("city")
    String city;

    public LogEntrySolrItem() {

    }
    
    public LogEntrySolrItem(LogEntry item) {
        this.entryId = item.getEntryId();
        this.pid = item.getIdentifier().getValue();
        this.ipAddress = item.getIpAddress();
        this.userAgent = item.getUserAgent();
        this.subject = item.getSubject().getValue();
        this.event = item.getEvent().xmlValue();
        this.dateLogged = item.getDateLogged();
        this.nodeIdentifier = item.getNodeIdentifier().getValue();
        
    }
    
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
    
    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setDateAggregated(Date dateAggregated) {
        this.dateAggregated = dateAggregated;
    }

    public Date getDateAggregated() {
        return dateAggregated;
    }

    public Date getDateLogged() {
        return dateLogged;
    }

    public void setDateLogged(Date dateLogged) {
        this.dateLogged = dateLogged;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getFormatId() {
        return formatId;
    }

    public void setFormatId(String formatId) {
        this.formatId = formatId;
    }   
    
    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    public void setNodeIdentifier(String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }
    
    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
    
    public String getRightsHolder() {
        return rightsHolder;
    }

    public void setRightsHolder(String rightsHolder) {
        this.rightsHolder = rightsHolder;
    }
    
    public BigInteger getSize() {
        return size;
    }

    public void setSize(BigInteger size) {
        this.size = size;
    }
    
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public boolean getIsPublic() {
        return isPublic;
    }

    public void setIsPublic(boolean isPublic) {
        this.isPublic = isPublic;
    }

    public List<String> getReadPermission() {
        return readPermission;
    }

    public void setReadPermission(List<String> readPermission) {
        this.readPermission = readPermission;
    }
}
