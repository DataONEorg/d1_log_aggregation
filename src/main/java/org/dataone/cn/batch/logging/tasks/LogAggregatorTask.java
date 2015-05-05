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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.dataone.client.MNode;
import org.dataone.cn.batch.logging.GeoIPService;
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
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
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
    private static final String geoIPdbName = Settings.getConfiguration().getString("LogAggregator.geoIPdbName");
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    private static AtomicNumber hzAtomicNumber;
    private String atomicNumberSequence = Settings.getConfiguration().getString("dataone.hazelcast.atomicNumberSequence");
    String hzSystemMetaMapString = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    static private int triggerIntervalPeriod = Settings.getConfiguration().getInt("LogAggregator.triggerInterval.period");
    static private String triggerIntervalPeriodField = Settings.getConfiguration().getString("LogAggregator.triggerInterval.periodField");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
    
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
        logger.info("LogAggregatorTask-" + d1NodeReference.getValue() + " Starting");
        LogAccessRestriction logAccessRestriction = new LogAccessRestriction();
        // COUNTER compliance of Event Log records is explained in the DataONE UsageStats document:
        //
        //     https://purl.dataone.org/architecture-dev/design/UsageStatistics.html#counter-compliance
        //
        // Event log record types to check for counter compliance
        HashSet<String> eventsToCheck = new HashSet<String>(Arrays.asList("read"));
        // List of web robots according to COUNTER standard
        ArrayList<String> fullWebRobotList = new ArrayList<String>();
        // Less strict list of web robots than COUNTER standard
        ArrayList<String> partialWebRobotList = new ArrayList<String>();
        // Cache for the read events, indexed by IPaddress. This cache will grow as the log entries
        // are processed, so it will be purged after it reaches a certain size.
        HashMap<String, DateTime> readEventCache = new HashMap<String, DateTime>();
        List<CSVRecord> webRobotIPs = new ArrayList<CSVRecord>();
        
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

            // Create a service to determine location name from IP address that was obtained from a log entry
            String dbFilename = Settings.getConfiguration().getString(
					"LogAggregator.geoIPdbName");
            // Initialize the GeoIPservice that derives location attributes from IP address. If the service
            // can't be initialized, then continue processing and the related fields will be set to blank
			GeoIPService geoIPsvc;

			try {
				geoIPsvc = GeoIPService.getInstance(dbFilename);
			} catch (Exception e) {
				throw new ServiceFailure(e.getMessage(), "Unable to initialize the GeoIP service");
			}

			// Get COUNTER compliance related parameters
            String fullWebRobotListFilePath = Settings.getConfiguration().getString("LogAggregator.fullWebRobotListFilePath");
            String partialWebRobotListFilePath = Settings.getConfiguration().getString("LogAggregator.partialWebRobotListFilePath");
            final int repeatVisitIntervalSeconds = Settings.getConfiguration().getInt("LogAggregator.repeatVisitIntervalSeconds");
            Boolean doWebRobotIPcheck = Settings.getConfiguration().getBoolean("LogAggregator.doWebRobotIPcheck");
            // Max number of entries in the read event cache
            int readEventCacheMax = Settings.getConfiguration().getInt("LogAggregator.readEventCacheMax");
            // Starting max number of entries in read event cache, can grow to readEventCacheMax before it is purged
            // of events older than 30 seconds
            int readEventCacheCurrentMax = 1000;
            
            BufferedReader inBuf;
            
            // Read in the list of web robots needed for COUNTER compliance checking
			String filePath = null;
			try {
				if (doWebRobotIPcheck) {
					String webRobotIPsFilePath = Settings.getConfiguration().getString("LogAggregator.webRobotIPsFilePath");
					String DataONE_IPsFilePath = Settings.getConfiguration().getString("LogAggregator.DataONE_IPsFilePath");
					// Read in the list of IP addresses associated with known web robots
					filePath = webRobotIPsFilePath;
					inBuf = new BufferedReader(new FileReader(filePath));
					CSVParser parser = new CSVParser(inBuf, CSVFormat.RFC4180);
					webRobotIPs = parser.getRecords();
					parser.close();
					// Add the list of DataONE CNs and MNs. Requests made from DataONE CNs or MNs
					// will be flagged as robot requests, so that these requests can be easily filtered
					// from usage statistics
					filePath = DataONE_IPsFilePath;
					inBuf = new BufferedReader(new FileReader(filePath));
					parser = new CSVParser(inBuf, CSVFormat.RFC4180);
					webRobotIPs.addAll(parser.getRecords());
					parser.close();
				}
				
	            String inLine;
				filePath = fullWebRobotListFilePath;
				inBuf = new BufferedReader(new FileReader(filePath));
				while ((inLine = inBuf.readLine()) != null) {
					fullWebRobotList.add(inLine);
				}
				inBuf.close();
				
				filePath = partialWebRobotListFilePath;
				inBuf = new BufferedReader(new FileReader(filePath));
				while ((inLine = inBuf.readLine()) != null) {
					partialWebRobotList.add(inLine);
				}
				inBuf.close();
			} catch (FileNotFoundException ex) {
				ex.printStackTrace();
				logger.error("LogAggregatorTask-"
						+ d1NodeReference.getValue() + " " + ex.getMessage()
						+ String.format("Unable to open file '%s' which is needed for COUNTER compliance checking", filePath));
				throw new ExecutionException(ex);
			} catch (IOException ex) {
				ex.printStackTrace();
				logger.error("LogAggregatorTask-"
						+ d1NodeReference.getValue() + " " + ex.getMessage()
						+ String.format("Error reading file '%s' which is needed for COUNTER compliance checking", filePath));
				throw new ExecutionException(ex);
			}
            
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
                } catch (ServiceFailure e) {
                    if (queryFailures < 5) {
                        try {
                            Thread.sleep(60000L);
                        } catch (InterruptedException ex) {
                            logger.warn(ex.getMessage());
                        }
                        tryAgain = true;
                        ++queryFailures;
                        totalFailure += queryFailures;
                        logger.warn("LogAggregatorTask-" + d1NodeReference.getValue() + " " + e.serialize(e.FMT_XML));
                        logger.warn("LogAggregatorTask-" + d1NodeReference.getValue() + " Failures this run = " + totalFailure);
                    } else {
                        throw e;
                    }
                    
                }
                if (readQueue != null && !readQueue.isEmpty()) {
                    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " found " + readQueue.size() + " entries");
                    List<LogEntrySolrItem> logEntrySolrItemList = new ArrayList<LogEntrySolrItem>(queryTotalLimit);
                    
                    String nodeId = null;
                    // process the LogEntries into Solr Items that can be persisted
                    // processing will add date aggregated, subjects allowed to read,
                    // and a unique identifier
                    for (LogEntry logEntry : readQueue) {
                        if (logEntry.getDateLogged().after(mostRecentLoggedDate)) {
                            mostRecentLoggedDate = logEntry.getDateLogged();
                        }

                        Date now = new Date();
                        Date dateAggregated = now;
                        LogEntrySolrItem solrItem = new LogEntrySolrItem(logEntry);
                        SystemMetadata systemMetadata = systemMetadataMap.get(logEntry.getIdentifier());
                        
                        // overwrite whatever the logEntry tells us here
                        // see redmine task #4099: NodeIds of Log entries may be incorrect
                        nodeId = d1NodeReference.getValue();
                        solrItem.setNodeIdentifier(nodeId);
                        solrItem.setDateAggregated(now);
                        solrItem.setDateUpdated(initializedDate);
            			/*
            			 * Fill in the solrItem fields for fields that are either obtained
            			 * from systemMetadata (i.e. formatId, size for a given pid) or are
            			 * derived from a field in the logEntry (i.e. location names,
            			 * geohash_* are derived from the ipAddress in the logEntry).
            			 */
            			solrItem.updateSysmetaFields(systemMetadata);
                    	solrItem.updateLocationFields(geoIPsvc);
                    	solrItem.setCOUNTERfields(partialWebRobotList, fullWebRobotList, readEventCache, eventsToCheck, repeatVisitIntervalSeconds, webRobotIPs, doWebRobotIPcheck);
                        // Purge the read event cache if it grows past a specified max value, however
                        // the number of items in the cache is determined by how far away they are from
                        // the time of the last event, so we need to check the purged size to see
                    	// if the the cache needs to be bigger.
                       	if (readEventCache.size() > readEventCacheCurrentMax) {
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " Purging Read Event Cache, size: " + readEventCache.size());
                            Iterator<Map.Entry<String, DateTime>> iterator = readEventCache.entrySet().iterator();
                            DateTime eventWindowStart = new DateTime(mostRecentLoggedDate).minusSeconds(repeatVisitIntervalSeconds+1);
                            while(iterator.hasNext()){
                                Map.Entry<String, DateTime> readEvent = iterator.next();                                
                    			if (readEvent.getValue().isBefore(eventWindowStart)) {
                                    iterator.remove();
                    			}
                            }
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " Read Event Cache size after purge: " + readEventCache.size());

                            // The eventCache purged of events that are older than repeatVisitIntervalSeconds minus the latest time.
                            // If the cache is larger max size after the purge, then adjust the max size to 5% greater than the
                            // purged size.
                            // The intent here is to intelligently increase the size of the cache so that it fits the current time window size,
                            // so that the cache isn't being purged continually.
                            if (readEventCache.size() > readEventCacheCurrentMax) {
                                float perc;
                                perc = (float) readEventCache.size() * (5.0f / 100.0f);
                                int newMax = readEventCache.size() + Math.round(perc);
                                // Try to increase cache size, but don't increase past max value
                                if (newMax < readEventCacheMax) {
                                	readEventCacheCurrentMax = newMax;
                                	logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + "Adjusting readEventCache max to: " + readEventCacheCurrentMax);
                                } else {
                                	readEventCacheCurrentMax = readEventCacheMax;
                                	logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + "Can't adjust readEventCache max to greater than: " + readEventCacheMax);
                                }
                            }
                    	}
                    	
                        /* Use the Member Node identifier combined with entryId for the Solr unique key. This natural key should be 
                         * globally unique, but also ensure that re-harvesting will not add duplicate records.
                         */
                        String id = nodeId + "." + logEntry.getEntryId();
                        solrItem.setId(id);
                        logEntrySolrItemList.add(solrItem);
                        //logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " added id: " + logEntry.getIdentifier().getValue() + " to solt item list.");
                    }
                    
    			    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " Done setting fields for entries, # of items to be added: " + logEntrySolrItemList.size());

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
                        logger.info("LogAggregatorTask-" + d1NodeReference.getValue() + " publishing " + publishEntrySolrItemList.size() + " itmes to hzLogEntryTopic");
                        hzLogEntryTopic.publish(publishEntrySolrItemList);

                        try {
                            // Simple way to throttle publishing of messages
                            // thread should sleep for 500MS
                            Thread.sleep(500L);
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
            logger.info("LogAggregatorTask-" + d1NodeReference.getValue() + " Complete");
            return mostRecentLoggedDate;
        } catch (InvalidToken ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (NotImplemented ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (InvalidRequest ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (ServiceFailure ex) {
            ex.printStackTrace();
            logger.error("LogAggregatorTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (EmptyStackException ex) {
            logger.warn("For some reason the date logQueryStack threw an empty exception but isEmpty reported false?");
            throw new ExecutionException(ex);
        } catch (NotAuthorized ex) {
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
    private List<LogEntry> retrieve(MNode mNode) throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken, QueryLimitException, EmptyStackException {
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
               boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
                if (!activateJob) {
                    logQueryStack.empty();
                    logger.warn("LogAggregatorTask-" + d1NodeReference.getValue() + "QueryStack is Emptied because LogAggregation has been de-activated");
                    throw new EmptyStackException();
                }
                // always execute for the first run (for start = 0)
                // otherwise skip because when the start is equal or greater
                // then total, then all objects have been harvested
                // based on information from metacat devs, first querying with
                // rows 0 will return quickly due to the
                // shortcut of not needing to perform paging
                if (start == 0) {
                    try {
                        logList = mNode.getLogRecords(logQueryDateRange.getFromDate(), logQueryDateRange.getToDate(), null, null, 0, 0);
                    } catch (NotAuthorized e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (InvalidRequest e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (NotImplemented e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (ServiceFailure e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (InvalidToken e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    }
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
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() +" late push start " + format.format(lateRecordDate.getFromDate()) + " end " + format.format(lateRecordDate.getToDate()));
                            logQueryStack.push(lateRecordDate);

                            LogQueryDateRange earlyRecordDate = new LogQueryDateRange(logQueryDateRange.getFromDate(), new Date(medianTime));
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + "early push start " + format.format(earlyRecordDate.getFromDate()) + " end " + format.format(earlyRecordDate.getToDate()));
                            logQueryStack.push(earlyRecordDate);
                            throw new QueryLimitException();
                        }
                    }
                }
                
                try {
                    logList = mNode.getLogRecords(session, logQueryDateRange.getFromDate(), logQueryDateRange.getToDate(), null, null, start, batchSize);
                } catch (NotAuthorized e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (InvalidRequest e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (NotImplemented e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (ServiceFailure e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (InvalidToken e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                }
                // if objectList is null or the count is 0 or the list is empty, then
                // there is nothing to process


                if ((logList != null)
                        && (logList.getCount() > 0)
                        && (logList.getLogEntryList() != null)
                        && (!logList.getLogEntryList().isEmpty())) {
                    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " harvested start " + format.format(logQueryDateRange.getFromDate()) + " end " + format.format(logQueryDateRange.getToDate()));
                    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " log harvested start#=" + logList.getStart() + " count=" + logList.getCount() + " total=" + logList.getTotal());
                    start += logList.getCount();
                    writeQueue.addAll(logList.getLogEntryList());
                    total = logList.getTotal();
                }
                
            } while ((logList != null) && (logList.getCount() > 0) && (start < total));
        } catch (EmptyStackException ex) {
            throw ex;
        }
        return writeQueue;
    }
}
