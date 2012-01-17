/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging.type;

import org.apache.solr.client.solrj.beans.Field;
import org.dataone.service.types.v1.LogEntry;
import java.util.Date;
/**
 *
 * @author waltz
 */
public class LogEntrySolrItem {

    @Field("id")
    private String id;

    @Field("dateAggregated")
    private Date dateAggregated;

    @Field("entryId")
    private String entryId;

    @Field("pid")
    private String pid;

    @Field("ipAddress")
    private String ipAddress;

    @Field("userAgent")
    private String userAgent;

    @Field("subject")
    private String subject;

    @Field("event")
    private String event;

    @Field("dateLogged")
    private Date dateLogged;

    @Field("nodeId")
    private String nodeIdentifier;

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

    public void setDateAggregated(Date dateAggregated) {
        this.dateAggregated = dateAggregated;
    }


}
