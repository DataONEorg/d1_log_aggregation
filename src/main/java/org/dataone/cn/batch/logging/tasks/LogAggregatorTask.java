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

package org.dataone.cn.batch.logging.tasks;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.batch.logging.LogAccessRestriction;
import org.dataone.cn.batch.logging.NodeRegistryPool;
import org.dataone.cn.batch.logging.exceptions.QueryLimitException;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.batch.logging.type.LogQueryDateRange;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.cn.ldap.NodeAccess;

import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.DateTimeMarshaller;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

/**
 * A distributable and executable task that retrieves a list of LogEntry
 * by calling log on a MN (or localhost cn)  and then publishes them on
 * the LogEntryTopic for processing. It will retrieve and submit
 * in batches of 1000 as the default.
 *
 * As an executable, it will return a date that is the latest LogLastAggregated
 *
 * If the retrieve method fails for any reason in the middle of aggregation
 * then the records on MN may become out of sync with those reported by the MN
 * 
 * @author waltz
 */
public class LogAggregatorTask implements Callable<Date>, Serializable {

    NodeReference d1NodeReference;
    private Session session;

    private Integer batchSize = Settings.getConfiguration().getInt("LogAggregator.logRecords_batch_size", 1000);
    private Integer queryTotalLimit = Settings.getConfiguration().getInt("LogAggregator.query_total_limit", 10000);
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    private static AtomicNumber hzAtomicNumber;
    private String atomicNumberSequence = Settings.getConfiguration().getString("dataone.hazelcast.atomicNumberSequence");
    String hzSystemMetaMapString = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    static private int triggerIntervalPeriod = Settings.getConfiguration().getInt("LogAggregator.triggerInterval.period");
    static private String triggerIntervalPeriodField = Settings.getConfiguration().getString("LogAggregator.triggerInterval.periodField");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    
    public LogAggregatorTask(NodeReference d1NodeReference) {
        this.d1NodeReference = d1NodeReference;

    }
    private Stack<LogQueryDateRange> logQueryStack = new Stack<LogQueryDateRange>();
    /**
     * Implement the Callable interface,  retrieves logging information from a D1 Node
     * and publishes a List<LogEntrySolrItem> to a hazelcast topic
     * 
     * The logging information retrieved will not be for the current day, but for 
     * a previous time period
     * 
     * @return Date
     * @throws Exception
     */
    @Override
    public Date call() throws ExecutionException {
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        LogAccessRestriction logAccessRestriction = new LogAccessRestriction();
        try {
            Boolean tryAgain = false;
            Integer queryFailures = 0;
            Integer totalFailure = 0;
            
            HazelcastInstance hzclient = HazelcastClientInstance.getHazelcastClient();
            // endDateTime is the latest datetime to which we wish to retrieve data
            DateTime endDateTime = new DateTime(DateTimeZone.UTC);

            // if offsets of the current time are not provided then assume midnight
            // offsets are really only useful for testing
            if (triggerIntervalPeriodField.equalsIgnoreCase("seconds")) {
                 // endDateTime is really now offset by a seconds found in config file
                endDateTime.minusSeconds(triggerIntervalPeriod);
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("minutes")) {
                // endDateTime is really now offset by a minutes found in config file
                endDateTime.minusMinutes(triggerIntervalPeriod);
            } else {
                // endDateTime is really midnight, or start of the day ( or however you would
                // like to think about the break of a day)
                // This means that the latest date to retrieve will be the last second of the
                // previous day since the toDate variable of logRecords
                // returns Records with a time stamp less than (<) the endDateTime provided
                // see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords
                endDateTime = endDateTime.withTime(0, 0, 0, 0);
            }


            IMap<Identifier, SystemMetadata> systemMetadataMap = hzclient.getMap(hzSystemMetaMapString);

            // we are going to write directly to ldap for the LogLastAggregated
            // because we do not want hazelcast to spam us about
            // all of these updates since we have a listener in HarvestSchedulingManager
            // that determines when updates/additions have occured and
            // re-adjusts scheduling
            NodeRegistryService nodeRegistryService = NodeRegistryPool.getInstance().getNodeRegistryService(d1NodeReference.getValue());
            NodeAccess nodeAccess =  nodeRegistryService.getNodeAccess();
            // logger is not  be serializable, but no need to make it transient imo
            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue());
            HazelcastInstance hazelcast = HazelcastInstanceFactory.getProcessingInstance();
            ITopic<List<LogEntrySolrItem>> hzLogEntryTopic = hazelcast.getTopic(hzLogEntryTopicName);
            // Need the LinkedHashMap to preserver insertion order
            Node d1Node = nodeRegistryService.getNode(d1NodeReference);
            Date lastMofidiedDate = nodeAccess.getLogLastAggregated(d1NodeReference);
            hzAtomicNumber = hazelcast.getAtomicNumber(atomicNumberSequence);
            if (lastMofidiedDate == null) {
                lastMofidiedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
            }

            Date mostRecentLoggedDate = new Date(lastMofidiedDate.getTime());
            
            // The last Harvested Date is always the most recent date of the last harvest
            // To get the next range of records, add a millisecond to the date
            // for use as the 'fromDate' parameter of the log query            
            MutableDateTime lastHarvestDateTime = new MutableDateTime(lastMofidiedDate);

            lastHarvestDateTime.addMillis(1);
            LogQueryDateRange initialLogQueryDateRange = new LogQueryDateRange(lastHarvestDateTime.toDate(), endDateTime.toDate());
            logQueryStack.push(initialLogQueryDateRange);
            
            String d1NodeBaseUrl = d1Node.getBaseURL();
            if (d1Node.getType().equals(NodeType.CN)) {
                d1NodeBaseUrl = Settings.getConfiguration().getString("LogAggregator.cn_base_url");
            }
            MNode mNode = new MNode(d1NodeBaseUrl);
            logger.info("LogAggregatorTask-" + d1NodeReference.getValue() + " starting retrieval " + d1NodeBaseUrl + " From " + DateTimeMarshaller.serializeDateToUTC(lastMofidiedDate) + " To " + DateTimeMarshaller.serializeDateToUTC(endDateTime.toDate()));
            do {
                List<LogEntry> readQueue = new ArrayList<LogEntry>();
                tryAgain = false;
                // read upto a 1000 objects (the default, but it can be overwritten)
                // from ListObjects and process before retrieving more
                try {
                    readQueue = this.retrieve(mNode);
                    queryFailures = 0;
                } catch (QueryLimitException e) {
                    tryAgain = true;
                } catch (Exception e) {
                    if (queryFailures < 5) {
                        try {
                            Thread.sleep(60000L);
                        } catch (InterruptedException ex) {
                            logger.warn(ex.getMessage());
                        }
                        tryAgain = true;
                        ++queryFailures;
                        totalFailure += queryFailures;
                        logger.info("LogAggregatorTask-" + d1NodeReference.getValue() + " Failures this run = " + totalFailure);
                    }
                    e.printStackTrace();
                }
                if (readQueue != null && !readQueue.isEmpty()) {
                    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " found " + readQueue.size() + " entries");
                    List<LogEntrySolrItem> logEntrySolrItemList = new ArrayList<LogEntrySolrItem>(queryTotalLimit);

                    // process the LogEntries into Solr Items that can be persisted
                    // processing will add date aggregated, subjects allowed to read,
                    // and a unique identifier
                    for (LogEntry logEntry : readQueue) {
                        if (logEntry.getDateLogged().after(mostRecentLoggedDate)) {
                            mostRecentLoggedDate = logEntry.getDateLogged();
                        }
                        Date now = new Date();
                        LogEntrySolrItem solrItem = new LogEntrySolrItem(logEntry);
                        solrItem.setDateAggregated(now);
                        SystemMetadata systemMetadata = systemMetadataMap.get(logEntry.getIdentifier());
                        if (systemMetadata != null) {
                            boolean isPublicSubject = false;
                            List<String> subjectsAllowedRead = logAccessRestriction.subjectsAllowedRead(systemMetadata);
                            solrItem.setIsPublic(isPublicSubject);
                            solrItem.setReadPermission(subjectsAllowedRead);
                        }
                        Long integral = new Long(now.getTime());
                        Long decimal = new Long(hzAtomicNumber.incrementAndGet());
                        String id = integral.toString() + "." + decimal.toString();
                        solrItem.setId(id);
                        logEntrySolrItemList.add(solrItem);
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
                            // thread should sleep for 250MS
                            Thread.sleep(250L);
                        } catch (InterruptedException ex) {
                            logger.warn("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.getMessage());
                        }
                        
                        startIndex = endIndex;
                    } while (endIndex < logEntrySolrItemList.size());
                    // Persist the most recent log date in LDAP
                    if (mostRecentLoggedDate.after(lastMofidiedDate)) {
                        nodeAccess.setLogLastAggregated(d1NodeReference, mostRecentLoggedDate);
                        logger.info("LogAggregatorTask-" + d1NodeReference.getValue() + " Latested Harvested Log Entry Date " + format.format(mostRecentLoggedDate));
                    }
                }
            } while (tryAgain || (!logQueryStack.isEmpty()));
 
            return mostRecentLoggedDate;
        } catch (ServiceFailure ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (NotFound ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.getMessage());
            throw new ExecutionException(ex);
        }
    }

    /*
     * performs the retrieval of the log records  from a DataONE node.
     * It retrieves the list in batches and should be called iteratively
     * until all log entries have been retrieved from a node.
     * 
     * @return List<LogEntry>
     */
    private List<LogEntry> retrieve(MNode mNode) throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken, QueryLimitException {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        List<LogEntry> writeQueue = new ArrayList<LogEntry>();
        try {
            LogQueryDateRange logQueryDateRange = logQueryStack.pop();


            int start = 0;
            int total = 0;
            Log logList = null;

            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " starting retrieval from " + start);
            do {
                // always execute for the first run (for start = 0)
                // otherwise skip because when the start is equal or greater
                // then total, then all objects have been harvested

                logList = mNode.getLogRecords(session, logQueryDateRange.getFromDate(), logQueryDateRange.getToDate(), null, null, start, batchSize);
                // if objectList is null or the count is 0 or the list is empty, then
                // there is nothing to process
                if (logList != null) {
                    // if the total records returned from the above query is greater than the
                    // limit we have placed on batch processing, then find the median date
                    // and try again.
                    // or if the date range of the query is less that one second, return the results as found
                    // even if they extend beyon the batch processing limit
                    if ((logList.getTotal() > queryTotalLimit)
                            && (logQueryDateRange.getToDate().getTime() - logQueryDateRange.getFromDate().getTime()) > 1000L) {
                        logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " discard pop start " + format.format(logQueryDateRange.getFromDate()) + " end " + format.format(logQueryDateRange.getToDate()));
                        long medianTime = (logQueryDateRange.getFromDate().getTime() + logQueryDateRange.getToDate().getTime()) / 2;

                        LogQueryDateRange lateRecordDate = new LogQueryDateRange(new Date(medianTime), logQueryDateRange.getToDate());
                        logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " late push start " + format.format(lateRecordDate.getFromDate()) + " end " + format.format(lateRecordDate.getToDate()));
                        logQueryStack.push(lateRecordDate);

                        LogQueryDateRange earlyRecordDate = new LogQueryDateRange(logQueryDateRange.getFromDate(), new Date(medianTime));
                        logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + "early push start " + format.format(earlyRecordDate.getFromDate()) + " end " + format.format(earlyRecordDate.getToDate()));
                        logQueryStack.push(earlyRecordDate);
                        throw new QueryLimitException();
                    }
                    if ((logList.getCount() > 0)
                            && (logList.getLogEntryList() != null)
                            && (!logList.getLogEntryList().isEmpty())) {
                        logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " harvested start " + format.format(logQueryDateRange.getFromDate()) + " end " + format.format(logQueryDateRange.getToDate()));
                        logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " log harvested start#=" + logList.getStart() + " count=" + logList.getCount() + " total=" + logList.getTotal());
                        start += logList.getCount();
                        writeQueue.addAll(logList.getLogEntryList());
                        total = logList.getTotal();
                    }
                }
            } while ((logList != null) && (logList.getCount() > 0) && (start < total));
        } catch (EmptyStackException ex) {
            ex.printStackTrace();
        }
        return writeQueue;
    }
}
