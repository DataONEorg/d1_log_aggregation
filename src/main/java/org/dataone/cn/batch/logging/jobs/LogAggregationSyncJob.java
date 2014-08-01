/**
 * This work was created by participants in the DataONE project, and is jointly copyrighted by participating
 * institutions in DataONE. For more information on DataONE, see our web site at http://dataone.org.
 *
 * Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * $Id$
 */
package org.dataone.cn.batch.logging.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.dataone.client.CNode;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.cn.ldap.ProcessingState;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.types.v1.Node;
import org.dataone.service.util.DateTimeMarshaller;
import org.dataone.solr.client.solrj.impl.CommonsHttpClientProtocolRegistry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Quartz Job that runs on a passive CN and queries the active CN for newer log records.
 * (Based on LogAggregationRecoveryJob)
 * 
 * @author slaughter
 */
@DisallowConcurrentExecution
public class LogAggregationSyncJob implements Job {

    SolrServer localhostSolrServer;
    private String localhostCNURL = Settings.getConfiguration().getString("D1Client.CN_URL");
    static final DateTimeFormatter zFmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");

    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {

        Log logger = LogFactory.getLog(LogAggregationSyncJob.class);
        String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");
        // Get the current 'active' CN (all others are 'passive')
        String activeCnIdentifier = Settings.getConfiguration().getString("cn.nodeId.active");
        NodeAccess nodeAccess = new NodeAccess();
        Map<NodeReference, Map<String, String>> nodeAccessMap;
        Date latestAggregatedDate = initializedDate;
        String syncQuery = null;

        // this will initialize the https protocol of the solrserver client
        // to read and send the x509 certificate
        try {
            CommonsHttpClientProtocolRegistry.createInstance();
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
        }
 
        NodeReference localNodeReference = new NodeReference();
        NodeReference activeCnReference = new NodeReference();

        localNodeReference.setValue(localCnIdentifier);
        activeCnReference.setValue(activeCnIdentifier);

        // get all the CNs with their logging status (combination of last date run and aggregation status)
        try {
            nodeAccessMap = nodeAccess.getCnLoggingStatus();
        } catch (ServiceFailure ex) {
            throw new IllegalStateException("Unable to initialize for sync: " + ex.getMessage());
        }
        
        String baseUrl = null;
		// Get the logsolr service URL for the active CN
		NodeRegistryService nodeRegistryService = new NodeRegistryService();
		Map<String, String> cnMap = nodeAccessMap.get(activeCnReference);
		if ((cnMap != null) && !cnMap.isEmpty()) {
			Node d1Node = null;
			try {
				d1Node = nodeRegistryService.getNode(activeCnReference);
			} catch (ServiceFailure e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotFound e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			baseUrl = d1Node.getBaseURL();
			logger.debug(localCnIdentifier + " found " + activeCnReference.getValue() + " with a state of "
					+ cnMap.get(NodeAccess.ProcessingStateAttribute));
		}

        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
        JobExecutionException jex = null;
        // Check that log aggregation is enabled on this CN
        boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
		try {
			nodeAccess.setProcessingState(localNodeReference, ProcessingState.Sync);
		} catch (ServiceFailure e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			if (activateJob) {
				Integer batchSize = Settings.getConfiguration().getInt(
						"LogAggregator.logRecords_batch_size");
				logger.info(localCnIdentifier + " executing sync with batch size "
						+ batchSize);
				String recoveryCnUrl = null;
				if (nodeAccessMap != null && !nodeAccessMap.isEmpty()) {
					// first look at our own state (have we ever run before???)
					Map<String, String> localCnMap = nodeAccessMap.get(localNodeReference);
					if (localCnMap != null && !localCnMap.isEmpty()) {
						// Query the local CN to determine the most recent
						// log entries that exist on the local CN - we will
						// request later records from the active CN.
						SolrQuery queryParams = new SolrQuery();
						queryParams.setQuery("dateAggregated:[* TO NOW]");
						queryParams.setSortField("dateAggregated",
								SolrQuery.ORDER.desc);
						queryParams.setStart(0);
						queryParams.setRows(1);
						try {
							CommonsHttpClientProtocolRegistry.createInstance();
						} catch (Exception ex) {
							ex.printStackTrace();
						}
				
						// must use https connection because the select
						// filter will require the cn node
						// subject in order to correctly configure the parameters
						String recoveringCnUrl = Settings.getConfiguration().getString("LogAggregator.solrUrl");
						CommonsHttpSolrServer recoveringSolrServer = new CommonsHttpSolrServer(
								recoveringCnUrl);
						QueryResponse queryResponse = recoveringSolrServer.query(queryParams);
						List<LogEntrySolrItem> logEntryList = queryResponse
								.getBeans(LogEntrySolrItem.class);
						if (!logEntryList.isEmpty()) {
							// there should only be one
							LogEntrySolrItem firstSolrItem = logEntryList.get(0);
							DateTime dt = new DateTime(firstSolrItem.getDateAggregated());
							DateTime dtUTC = dt.withZone(DateTimeZone.UTC);
							latestAggregatedDate = dtUTC.toDate();
							// provide a buffer in case we missed entries
							// because of mis-ordering...
							dtUTC = dtUTC.minusSeconds(60);
							syncQuery = "dateAggregated:[" + zFmt.print(dtUTC) + " TO NOW]";
						} else {
							logger.warn("localhost solr query should have returned rows but it did not");
							syncQuery = "dateAggregated:[* TO NOW]";
						}
						logger.warn("Local host map (LDAP) for node " + localCnIdentifier + " is empty or non-existant");
						syncQuery = "dateAggregated:[* TO NOW]";
					}
				}
                 
                recoveryCnUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/cn"));
                recoveryCnUrl += Settings.getConfiguration().getString("LogAggregator.solrUrlPath");
                CommonsHttpSolrServer recoverySolrServer = new CommonsHttpSolrServer(recoveryCnUrl);
                recoverySolrServer.setConnectionTimeout(30000);
                recoverySolrServer.setSoTimeout(30000);
                recoverySolrServer.setMaxRetries(1);

                List<LogEntrySolrItem> logEntryList = null;

                Integer start = 0;
                long total = 0;
                logger.info(localCnIdentifier +  " Starting sync from " + recoveryCnUrl);
                Date lastLogAggregatedDate = nodeAccess.getLogLastAggregated(localNodeReference);
                Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
                Boolean assignDate = false;
                // only assign the lastLogAggregated Date if logAggregation
                // has never run successfully before and this
                // recovery job is running.
                if (lastLogAggregatedDate == null) {
                    lastLogAggregatedDate = initializedDate;
                    assignDate = true;
                } else if (lastLogAggregatedDate.compareTo(initializedDate) == 0) {
                    assignDate = true;
                }

                do {
                    // read up to a 1000 objects (the default, but it can be overwritten)
                    // from Log and process before retrieving more
                    // find out what the last log record is to get the date from it for recovery purposes
                    SolrQuery queryParams = new SolrQuery();
                    queryParams.setQuery(syncQuery);
                    queryParams.setSortField("dateAggregated", SolrQuery.ORDER.asc);
                    queryParams.setStart(start);
                    queryParams.setRows(batchSize);

                    QueryResponse queryResponse = recoverySolrServer.query(queryParams);

                    logEntryList = queryResponse.getBeans(LogEntrySolrItem.class);
                    if (!logEntryList.isEmpty()) {
                    	// Add entry to the local CN Solr index
                        localhostSolrServer.addBeans(logEntryList);
                        localhostSolrServer.commit();
                        if (assignDate) {
                            for (LogEntrySolrItem logEntry : logEntryList) {
                                if (logEntry.getDateLogged().after(lastLogAggregatedDate)) {
                                    lastLogAggregatedDate = logEntry.getDateLogged();
                                }
                            }
                        }
                        start += logEntryList.size();
                    }
                } while ((logEntryList != null) && (!logEntryList.isEmpty()));
                if (assignDate) {
                    nodeAccess.setLogLastAggregated(localNodeReference, lastLogAggregatedDate);
                }
                logger.info(localCnIdentifier +  " LogAggregation is fully synced on node " + localCnIdentifier);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
        }
        if (activateJob) {
            try {
                nodeAccess.setProcessingState(localNodeReference, ProcessingState.Active);
                logger.debug(localCnIdentifier + " Processing is now Active");
            } catch (ServiceFailure ex) {
                logger.error(ex);
            }
        } else {
            // job was inactivated for some reason, so lets try it again
            jex = new JobExecutionException();
            jex.refireImmediately();
        }
        if (jex != null) {
            try {
                // Want this to refire  after this thread ends, but after some delay
                // lets wait 5 minutes
                Thread.sleep(300000L);
                throw jex;
            } catch (InterruptedException ex) {
                logger.warn(localCnIdentifier + " Tried to sleep before recovery job refires. But Interrupted :" + ex.getMessage());
            }
        }

    }

    public SolrServer getLocalhostSolrServer() {
        return localhostSolrServer;
    }

    public void setLocalhostSolrServer(SolrServer localhostSolrServer) {
        this.localhostSolrServer = localhostSolrServer;
    }
}
