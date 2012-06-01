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

import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
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
import org.dataone.service.types.v1.LogEntry;

import org.dataone.service.types.v1.Node;
import org.dataone.service.util.DateTimeMarshaller;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Quartz Job that starts off the recovery of log entries from a from a CN
 *
 * Job may not be executed concurrently for a coordinating node
 *
 * An executable task that will query a CN that is fully recovered and pull all the dates of items needed back to the
 * localhost The process will then call LogEntryIndexTask for the batches of LogEntrySolrItems that are returned
 *
 * The main reason to keep this a separate class, instead of integrating the code directly into
 * LogAggregationRecoverJob, is for testing. Another, but less important reason, is that it conforms to the pattern of
 * Quartz jobs spawning off a separate task that performs the work The main difference is that this task is not
 * distributed.
 *
 * If the task fails for any reason in the middle of recovery then the records on CN may become out of sync with those
 * reported by the MN
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class LogAggregationRecoverJob implements Job {

    String recoveryQuery;
    SolrServer localhostSolrServer;
    Date latestRecoverableDate;

    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {

        // do not submit the localCNIdentifier to Hazelcast for execution
        // rather execute it locally on the machine
        boolean foundRecoveringNode = false;
        String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");
        NodeReference localNodeReference = new NodeReference();
        localNodeReference.setValue(localCnIdentifier);
        SimpleDateFormat format =
                new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
        Log logger = LogFactory.getLog(LogAggregationRecoverJob.class);

        JobExecutionException jex = null;
        NodeRegistryService nodeRegistryService = new NodeRegistryService();
        NodeAccess nodeAccess = new NodeAccess();
        boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
        try {

            if (activateJob) {
                Integer batchSize = Settings.getConfiguration().getInt("LogAggregator.logRecords_batch_size");
                logger.info(localCnIdentifier + " executing with batch size " + batchSize);
                String recoveryCnUrl = null;
                Future future = null;

                do {
                    Map<NodeReference, Map<String, String>> recoveryMap = nodeAccess.getCnLoggingStatus();
                    for (NodeReference cnReference : recoveryMap.keySet()) {
                        if (!cnReference.equals(localNodeReference)) {
                            Map<String, String> cnMap = recoveryMap.get(cnReference);
                            if ((cnMap != null) && !cnMap.isEmpty()) {
                                Node d1Node = nodeRegistryService.getNode(cnReference);
                                String baseUrl = d1Node.getBaseURL();
                                logger.debug(localCnIdentifier +  " found " + cnReference.getValue() + " with a state of " + cnMap.get(NodeAccess.ProcessingStateAttribute));
                                ProcessingState logProcessingState = ProcessingState.convert(cnMap.get(NodeAccess.ProcessingStateAttribute));
                                switch (logProcessingState) {
                                    case Active: {
                                        // So it is true, a CN is running logAggregation
                                        // this localhost has never run before, but another CN is running
                                        // we need to replicate all logs from other CN to this one

                                        recoveryCnUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/cn"));
                                        recoveryCnUrl += Settings.getConfiguration().getString("LogAggregator.solrUrlPath");
                                        // a machine may reportedly be active, but it actually offline becuase of a
                                        // system failure. Check to make certain that the system is really active
                                        // ping the damn thing to make certain it responds before we break!
                                        CNode cnode = new CNode(baseUrl);
                                        try {
                                            cnode.ping();
                                        } catch (Exception e) {
                                            // not active, node is down
                                            recoveryCnUrl = null;
                                        }

                                        break;
                                    }
                                    case Recovery: {
                                        // Don't bother with this logic if an Active CN has been found
                                        if (recoveryCnUrl == null) {
                                            // a machine may reportedly be active, but it is actually offline because of a
                                            // system failure. Check to make certain that the system is indeed online
                                            // ping it
                                            CNode cnode = new CNode(baseUrl);
                                            try {
                                                cnode.ping();
                                                String recoveringCnUrl = baseUrl.substring(0, baseUrl.lastIndexOf("/cn"));
                                                recoveringCnUrl += Settings.getConfiguration().getString("LogAggregator.solrUrlPath");

                                                CommonsHttpSolrServer recoveringSolrServer = new CommonsHttpSolrServer(recoveringCnUrl);
                                                recoveringSolrServer.setConnectionTimeout(30000);
                                                recoveringSolrServer.setSoTimeout(30000);
                                                recoveringSolrServer.setMaxRetries(1);
                                                // initialize it incase the node in recovery has an empty logEntryList
                                                Date cnRecoveryDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
                                                SolrQuery queryParams = new SolrQuery();
                                                queryParams.setQuery("dateAggregated:[* TO NOW]");
                                                queryParams.setSortField("dateAggregated", SolrQuery.ORDER.desc);
                                                queryParams.setStart(0);
                                                queryParams.setRows(1);
                                                // get the last date that the recoverying node knows about
                                                QueryResponse queryResponse = recoveringSolrServer.query(queryParams);
                                                List<LogEntrySolrItem> logEntryList = queryResponse.getBeans(LogEntrySolrItem.class);
                                                if (!logEntryList.isEmpty()) {
                                                    // there should only be one
                                                    LogEntrySolrItem firstSolrItem = logEntryList.get(0);
                                                    DateTime dt = new DateTime(firstSolrItem.getDateAggregated());

                                                    DateTime dtUTC = dt.withZone(DateTimeZone.UTC);
                                                    cnRecoveryDate = dtUTC.toDate();

                                                }
                                                logger.debug(localCnIdentifier + " May recover from " + recoveringCnUrl + " from date " + format.format(cnRecoveryDate) + " if it is after " + format.format(latestRecoverableDate));
                                                // One of the nodes is actively being recovered
                                                // make certain that the last time the other CN ran
                                                // is after the date when this cn last ran
                                                // if all CN's are in recovery, then the one with the
                                                // latest date should not attempt recovery from the others

                                                if (cnRecoveryDate.after(latestRecoverableDate)) {
                                                    foundRecoveringNode = true;
                                                    logger.debug(localCnIdentifier + " Found Recovering Node");
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                                // not active, node is down
                                                logger.error(localCnIdentifier +  " Node must be down " + e.getMessage());
                                                foundRecoveringNode = false;
                                            }

                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (recoveryCnUrl == null && foundRecoveringNode) {
                        // so a node is in recovery. sleep for a while and the check again until it becomes active
                        //
                        Thread.sleep(61000L);
                    }
                } while (recoveryCnUrl == null && foundRecoveringNode);

                if (recoveryCnUrl == null) {
                    throw new Exception(localCnIdentifier +  " Unable to complete recovery because no nodes are available for recovery process");
                }
                // It would be nice to be able to inject the SolrServer. and implement
                // a mock SolrServer for testing
                // CommonsHttpSolrServer is serializable and could be passed in...
                // But we have to change the URL based on the servers available
                CommonsHttpSolrServer recoverySolrServer = new CommonsHttpSolrServer(recoveryCnUrl);
                recoverySolrServer.setConnectionTimeout(30000);
                recoverySolrServer.setSoTimeout(30000);
                recoverySolrServer.setMaxRetries(1);

                List<LogEntrySolrItem> logEntryList = null;

                Integer start = 0;
                long total = 0;
                logger.info(localCnIdentifier +  " Starting recovery from " + recoveryCnUrl);
                Date lastLogAggregatedDate = nodeAccess.getLogLastAggregated(localNodeReference);
                Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
                Boolean assignDate = false;
                if (lastLogAggregatedDate == null) {
                    lastLogAggregatedDate = initializedDate;
                    assignDate = true;
                } else if (lastLogAggregatedDate.compareTo(initializedDate) == 0) {
                    assignDate = true;
                }

                do {
                    // read upto a 1000 objects (the default, but it can be overwritten)
                    // from Log and process before retrieving more
                    // find out what the last log record is to get the date from it for recovery purposes
                    SolrQuery queryParams = new SolrQuery();
                    queryParams.setQuery(recoveryQuery);
                    queryParams.setSortField("dateAggregated", SolrQuery.ORDER.desc);
                    queryParams.setStart(start);
                    queryParams.setRows(batchSize);

                    QueryResponse queryResponse = recoverySolrServer.query(queryParams);

                    logEntryList = queryResponse.getBeans(LogEntrySolrItem.class);
                    if (!logEntryList.isEmpty()) {
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
                logger.info(localCnIdentifier +  " LogAggregation is fully Recovered");
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

    public String getRecoveryQuery() {
        return recoveryQuery;
    }

    public void setRecoveryQuery(String recoveryQuery) {
        this.recoveryQuery = recoveryQuery;
    }

    public SolrServer getLocalhostSolrServer() {
        return localhostSolrServer;
    }

    public void setLocalhostSolrServer(SolrServer localhostSolrServer) {
        this.localhostSolrServer = localhostSolrServer;
    }

    public Date getLatestRecoverableDate() {
        return latestRecoverableDate;
    }

    public void setLatestRecoverableDate(Date latestRecoverableDate) {
        this.latestRecoverableDate = latestRecoverableDate;
    }
}
