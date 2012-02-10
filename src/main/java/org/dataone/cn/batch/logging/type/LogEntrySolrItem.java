/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging.type;

import java.io.Serializable;
import org.apache.solr.client.solrj.beans.Field;
import org.dataone.service.types.v1.LogEntry;
import java.util.Date;
/**
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



}
