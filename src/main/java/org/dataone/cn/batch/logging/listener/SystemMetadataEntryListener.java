/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.listener;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.x500.X500Principal;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.util.Constants;

/**
 *
 * @author waltz
 */
public class SystemMetadataEntryListener implements EntryListener<Identifier, SystemMetadata> {

    private static Logger logger = Logger.getLogger(SystemMetadataEntryListener.class.getName());
    private HazelcastClient hzClient;
    private HazelcastInstance hazelcast;
    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    private static final String HZ_LOGENTRY_TOPICNAME = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    private IMap<Identifier, SystemMetadata> systemMetadata;
    private SolrServer localhostSolrServer;
    private ITopic<List<LogEntrySolrItem>> hzLogEntryTopic;

    public SystemMetadataEntryListener() {
    }

    public void start() {
        logger.info("starting systemMetadata entry listener...");
        logger.info("System Metadata value: " + HZ_SYSTEM_METADATA);

        this.hzClient = HazelcastClientInstance.getHazelcastClient();
        this.systemMetadata = this.hzClient.getMap(HZ_SYSTEM_METADATA);
        this.systemMetadata.addEntryListener(this, true);
        this.hzLogEntryTopic = hazelcast.getTopic(HZ_LOGENTRY_TOPICNAME);
        logger.info("System Metadata size: " + this.systemMetadata.size());
    }

    public void stop() {
        logger.info("stopping index task generator entry listener...");
        this.systemMetadata.removeEntryListener(this);
    }

    @Override
    public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {
        if (event.getKey() != null && event.getValue() != null) {
            SystemMetadata systemMetadata = event.getValue();
            logger.info("UPDATE EVENT - index task generator - system metadata callback invoked on pid: "
                    + event.getKey().getValue());
            List<LogEntrySolrItem> publishLogEntryList = retrieveLogEntries(event.getKey().getValue());
            if (!publishLogEntryList.isEmpty()) {
                processLogEntries(publishLogEntryList, systemMetadata);
            }
        }
    }

    @Override
    public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
        if (event.getKey() != null && event.getValue() != null) {
            SystemMetadata systemMetadata = event.getValue();
            if (systemMetadata.getSerialVersion().longValue() == 1) {
                logger.info("ADD EVENT - index task generator - system metadata callback invoked on pid: "
                        + event.getKey().getValue());
                List<LogEntrySolrItem> publishLogEntryList = retrieveLogEntries(event.getKey().getValue());
                if (!publishLogEntryList.isEmpty()) {
                    processLogEntries(publishLogEntryList, systemMetadata);
                }
            }
        }
    }

    private List<LogEntrySolrItem> retrieveLogEntries(String pid) {
        List<LogEntrySolrItem> completeLogEntrySolrItemList = new ArrayList<LogEntrySolrItem>();
        SolrQuery queryParams = new SolrQuery();
        queryParams.setQuery("pid: " + pid);
        queryParams.setStart(0);
        queryParams.setRows(1000);

        QueryResponse queryResponse;
        try {
            queryResponse = localhostSolrServer.query(queryParams);

            List<LogEntrySolrItem> logEntrySolrItemList = queryResponse.getBeans(LogEntrySolrItem.class);
            completeLogEntrySolrItemList.addAll(logEntrySolrItemList);
            int currentTotal = logEntrySolrItemList.size();
            long totalResults = queryResponse.getResults().getNumFound();
            if (currentTotal < totalResults) {
                do {
                    queryParams.setStart(currentTotal);
                    queryResponse = localhostSolrServer.query(queryParams);
                    logEntrySolrItemList = queryResponse.getBeans(LogEntrySolrItem.class);
                    completeLogEntrySolrItemList.addAll(logEntrySolrItemList);
                    currentTotal += logEntrySolrItemList.size();

                } while (currentTotal < totalResults);
            }
        } catch (SolrServerException ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
        }
        return completeLogEntrySolrItemList;
    }

    private void processLogEntries(List<LogEntrySolrItem> logEntrySolrItemList, SystemMetadata systemMetadata) {
        boolean isPublicSubject = false;
        Subject publicSubject = new Subject();
        publicSubject.setValue(Constants.SUBJECT_PUBLIC);
        List<String> subjectsAllowedRead = new ArrayList<String>();
        // RightsHolder always has read permission
        // even if SystemMetadata does not have an AccessPolicy
        Subject rightsHolder = systemMetadata.getRightsHolder();
        if ((rightsHolder != null) && !(rightsHolder.getValue().isEmpty())) {
            X500Principal principal = new X500Principal(rightsHolder.getValue());
            String standardizedName = principal.getName(X500Principal.RFC2253);
            subjectsAllowedRead.add(standardizedName);
        }

        if (systemMetadata.getAccessPolicy() != null) {
            List<AccessRule> allowList = systemMetadata.getAccessPolicy().getAllowList();

            for (AccessRule accessRule : allowList) {
                List<Subject> subjectList = accessRule.getSubjectList();
                for (Subject accessSubject : subjectList) {
                    if (accessSubject.equals(publicSubject)) {
                        // set Public access boolean on record
                        isPublicSubject = true;
                    } else {
                        // add subject as having read access on the record
                        X500Principal principal = new X500Principal(accessSubject.getValue());
                        String standardizedName = principal.getName(X500Principal.RFC2253);
                        subjectsAllowedRead.add(standardizedName);
                    }
                }
            }

        } else {
            logger.warn("SystemMetadata with pid " + systemMetadata.getIdentifier().getValue() + " does not have an access policy");
        }
        for (LogEntrySolrItem solrItem : logEntrySolrItemList) {
            solrItem.setIsPublic(isPublicSubject);
            solrItem.setReadPermission(subjectsAllowedRead);

        }
        // publish 100 at a time, do not overwhelm the
        // network with massive packets, or too many small packets
        int startIndex = 0;
        int endIndex = 0;
        do {
            endIndex += 100;
            if (logEntrySolrItemList.size() < endIndex) {
                endIndex = logEntrySolrItemList.size();
            }
            List<LogEntrySolrItem> publishEntrySolrItemList = new ArrayList<LogEntrySolrItem>(100);
            publishEntrySolrItemList.addAll(logEntrySolrItemList.subList(startIndex, endIndex));
            hzLogEntryTopic.publish(publishEntrySolrItemList);
            try {
                // Simple way to throttle publishing of messages
                // thread should sleep of 250MS
                Thread.sleep(250L);
            } catch (InterruptedException ex) {
                logger.warn(ex.getMessage());
            }
            startIndex = endIndex;
        } while (endIndex < logEntrySolrItemList.size());
    }

    @Override
    public void entryEvicted(EntryEvent<Identifier, SystemMetadata> arg0) {
    }

    @Override
    public void entryRemoved(EntryEvent<Identifier, SystemMetadata> arg0) {
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }
}
