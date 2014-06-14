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

package org.dataone.cn.batch.logging.listener;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.net.URLCodec;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.dataone.cn.batch.logging.GeoIPService;
import org.dataone.cn.batch.logging.LogAccessRestriction;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.solr.client.solrj.impl.CommonsHttpClientProtocolRegistry;

/**
 * Access to Objects may change
 * Listen to the systemMetadata maps and if the accessibility of the object
 * has changed, then all log records associated with the object
 * must also change
 * 
 * All log records will need to be periodically swept for inconsistency
 * should this listener go down
 * 
 * @author waltz
 */
public class SystemMetadataEntryListener implements EntryListener<Identifier, SystemMetadata> {

    private static Logger logger = Logger.getLogger(SystemMetadataEntryListener.class.getName());
    private static HazelcastClient hzclient;
    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    private static final String HZ_LOGENTRY_TOPICNAME = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    private IMap<Identifier, SystemMetadata> systemMetadata;
    private BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue;
    private SolrServer localhostSolrServer;
    //private LogAccessRestriction logAccessRestriction;
    private URLCodec urlCodec = new URLCodec("UTF-8");
    public SystemMetadataEntryListener() {
      String cnURL = Settings.getConfiguration().getString("D1Client.CN_URL");
        String localhostCNURL = cnURL.substring(0, cnURL.lastIndexOf("/cn"));
        localhostCNURL += Settings.getConfiguration().getString("LogAggregator.solrUrlPath");
        
        try {
            localhostSolrServer = new CommonsHttpSolrServer(localhostCNURL);
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
            throw new RuntimeException();
        }
        try {
            CommonsHttpClientProtocolRegistry.createInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
            throw new RuntimeException();
        }
        //logAccessRestriction = new LogAccessRestriction();
    }

    public void start() {
        logger.info("starting systemMetadata entry listener...");
        logger.info("System Metadata value: " + HZ_SYSTEM_METADATA);
  
        hzclient = HazelcastClientFactory.getStorageClient();
        this.systemMetadata = hzclient.getMap(HZ_SYSTEM_METADATA);
        this.systemMetadata.addEntryListener(this, true);
        logger.info("System Metadata size: " + this.systemMetadata.size());
    }

    public void stop() {
        logger.info("stopping index task generator entry listener...");
        this.systemMetadata.removeEntryListener(this);
    }

    @Override
    public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {
         boolean activateJob  = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
        if (event.getKey() != null && event.getValue() != null && activateJob) {
            SystemMetadata systemMetadata = event.getValue();
            logger.debug("UPDATE EVENT - index task generator - system metadata callback invoked on pid: "
                    + event.getKey().getValue());
            List<LogEntrySolrItem> publishLogEntryList = retrieveLogEntries(event.getKey().getValue());
            if (!publishLogEntryList.isEmpty()) {
                processLogEntries(publishLogEntryList, systemMetadata);
            }
        }
    }

    @Override
    public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
         boolean activateJob  = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
        if (event.getKey() != null && event.getValue() != null & activateJob) {
            SystemMetadata systemMetadata = event.getValue();
            if (systemMetadata.getSerialVersion().longValue() == 1) {
                logger.debug("ADD EVENT - index task generator - system metadata callback invoked on pid: "
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

        String escapedPID = ClientUtils.escapeQueryChars(pid);
        logger.debug(escapedPID);

        SolrQuery queryParams = new SolrQuery();
        queryParams.setQuery("pid:" + escapedPID );
        queryParams.setStart(0);
        queryParams.setRows(1000);

        QueryResponse queryResponse;
        try {
            logger.debug(queryParams.getQuery());
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
        logger.debug("returning # log entries to modify: " + completeLogEntrySolrItemList.size());
        return completeLogEntrySolrItemList;
    }

    private void processLogEntries(List<LogEntrySolrItem> logEntrySolrItemList, SystemMetadata systemMetadata) {

    	String dbFilename = Settings.getConfiguration().getString(
				"LogAggregator.geoIPdbName");
        Date now = new Date();

        //List<String> subjectsAllowedRead = logAccessRestriction.subjectsAllowedRead(systemMetadata);

		for (LogEntrySolrItem solrItem : logEntrySolrItemList) {
			/*
			 * Fill in the solrItem fields for fields that are either obtained
			 * from systemMetadata.
			 */
			solrItem.updateSysmetaFields(systemMetadata);
			solrItem.setDateUpdated(now);
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
            
            try {
                indexLogEntryQueue.offer(publishEntrySolrItemList, 30L, TimeUnit.SECONDS);
                logger.info("OFFERING - " + publishEntrySolrItemList.size() + " entries for pid: " +
                    systemMetadata.getIdentifier().getValue());
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
    
    public BlockingQueue getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }
   
}
