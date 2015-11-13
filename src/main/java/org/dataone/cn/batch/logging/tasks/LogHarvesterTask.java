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
package org.dataone.cn.batch.logging.tasks;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dataone.cn.batch.logging.GeoIPService;
import org.dataone.cn.batch.logging.LogAccessRestriction;
import org.dataone.cn.batch.logging.NodeRegistryPool;
import org.dataone.cn.batch.logging.exceptions.QueryLimitException;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.batch.logging.type.LogQueryDateRange;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.DateTimeMarshaller;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import org.dataone.cn.batch.logging.NodeHarvester;
import org.dataone.cn.batch.logging.SolrClientManager;
import org.dataone.cn.hazelcast.HazelcastClientFactory;

/**
 * A executable task that retrieves a list of LogEntry by calling log on a MN or CN and
 * then publishes them on the LogEntryTopic for processing. It will retrieve and submit in batches of 1000 as the
 * default.
 *
 * As an executable, it will return a date that is the latest LogLastAggregated
 *
 * If the retrieve method fails for any reason in the middle of aggregation then the records on MN may become out of
 * sync with those reported by the MN
 *
 * @author waltz
 */
public class LogHarvesterTask  {

    private static final long serialVersionUID = 10000000;
    NodeHarvester nodeHarvester;
    NodeReference d1NodeReference;

    private Integer queryTotalLimit = Settings.getConfiguration().getInt("LogAggregator.query_total_limit", 10000);
    private static final String geoIPdbName = Settings.getConfiguration().getString("LogAggregator.geoIPdbName");

    String hzSystemMetaMapString = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    static private int triggerIntervalPeriod = Settings.getConfiguration().getInt("LogAggregator.triggerInterval.period", 24);
    static private String triggerIntervalPeriodField = Settings.getConfiguration().getString("LogAggregator.triggerInterval.periodField", "default");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
    private static final int MAX_OFFERED_ATTEMPTS = 5;
    private static final int MAX_LIST_LOG_ENTRY_SOLR_ITEMS = 500;
    public LogHarvesterTask(NodeHarvester d1NodeHarvester) {
        this.nodeHarvester = d1NodeHarvester;
        this.d1NodeReference = d1NodeHarvester.getNodeReference();

    }
    private Stack<LogQueryDateRange> logQueryStack = new Stack<LogQueryDateRange>();
    /**
     * Implement the Callable interface, retrieves logging information from a D1 Node and publishes a
     * List<LogEntrySolrItem> to a hazelcast topic
     *
     * The logging information retrieved will not be for the current day, but for a previous time period
     *
     * @return Date
     * @throws Exception
     */

    public Date harvest() throws Exception {

        Logger logger = Logger.getLogger(LogHarvesterTask.class.getName());
        SolrClientManager solrClientManager = SolrClientManager.getInstance();
        logger.info("LogHarvesterTask-" + d1NodeReference.getValue() + " Starting");
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

            HazelcastClient hzStorageClient = HazelcastClientFactory.getStorageClient();
            // endDateTime is the latest datetime to which we wish to retrieve data
            DateTime endDateTime = new DateTime(DateTimeZone.UTC);

            // if offsets of the current time are not provided then assume midnight
            // offsets are really only useful for testing
            if (triggerIntervalPeriodField.equalsIgnoreCase("seconds")) {
                // endDateTime is really now offset by a seconds found in config file
                endDateTime = endDateTime.minusSeconds(triggerIntervalPeriod);
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("minutes")) {
                // endDateTime is really now offset by a minutes found in config file
                endDateTime = endDateTime.minusMinutes(triggerIntervalPeriod);
            } else {
                // endDateTime is really midnight, or start of the day ( or however you would
                // like to think about the break of a day)
                // This means that the latest date to retrieve will be the last second of the
                // previous day since the toDate variable of logRecords
                // returns Records with a time stamp less than (<) the endDateTime provided
                // see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords
                endDateTime = endDateTime.withTime(0, 0, 0, 0);
            }

            IMap<Identifier, SystemMetadata> systemMetadataMap = hzStorageClient.getMap(hzSystemMetaMapString);

            // we are going to write directly to ldap for the LogLastAggregated
            // because we do not want hazelcast to spam us about
            // all of these updates since we have a listener in HarvestSchedulingManager
            // that determines when updates/additions have occured and
            // re-adjusts scheduling
            NodeRegistryService nodeRegistryService = NodeRegistryPool.getInstance().getNodeRegistryService(d1NodeReference.getValue());
            NodeAccess nodeAccess = nodeRegistryService.getNodeAccess();
            // logger is not  be serializable, but no need to make it transient imo

            // Need the LinkedHashMap to preserver insertion order
            Node d1Node = nodeRegistryService.getNode(d1NodeReference);
            Date lastMofidiedDate = nodeAccess.getLogLastAggregated(d1NodeReference);

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
                logger.error("LogHarvesterTask-"
                        + d1NodeReference.getValue() + " " + ex.getMessage()
                        + String.format("Unable to open file '%s' which is needed for COUNTER compliance checking", filePath));
                throw new ExecutionException(ex);
            } catch (IOException ex) {
                ex.printStackTrace();
                logger.error("LogHarvesterTask-"
                        + d1NodeReference.getValue() + " " + ex.getMessage()
                        + String.format("Error reading file '%s' which is needed for COUNTER compliance checking", filePath));
                throw new ExecutionException(ex);
            }

            logger.info("LogHarvesterTask-" + d1NodeReference.getValue() + " starting retrieval From " + DateTimeMarshaller.serializeDateToUTC(lastMofidiedDate) + " To " + DateTimeMarshaller.serializeDateToUTC(endDateTime.toDate()));
            do {
                // We only want to publish log records harvested from the Member Node if they pass all tests
                // such as the identifier not being blank, etc. The logEntrySolrItem list holds records
                // harvested from the MN and logEntrySolrItemList holds solr records that have passed 
                // the data integrity checks below.
                List<LogEntrySolrItem> logEntrySolrItemList = new ArrayList<LogEntrySolrItem>();
                List<LogEntrySolrItem> logEntrySolrItemListToPublish = new ArrayList<LogEntrySolrItem>();
                tryAgain = false;
                // read upto a 1000 objects (the default, but it can be overwritten)
                // from ListObjects and process before retrieving more
                try {
                    logEntrySolrItemList = nodeHarvester.harvest(logQueryStack, queryTotalLimit);
                    queryFailures = 0;
                } catch (QueryLimitException e) {
                    tryAgain = true;
                } catch (BaseException e) {
                    if (queryFailures < 5) {
                        try {
                            Thread.sleep(60000L);
                        } catch (InterruptedException ex) {
                            logger.warn(ex.getMessage());
                        }
                        tryAgain = true;
                        ++queryFailures;
                        totalFailure += queryFailures;
                        logger.warn("LogHarvesterTask-" + d1NodeReference.getValue() + " " + e.serialize(e.FMT_XML));
                        logger.warn("LogHarvesterTask-" + d1NodeReference.getValue() + " Failures this run = " + totalFailure);
                    } else {
                        throw e;
                    }

                }
                if (logEntrySolrItemList != null && !logEntrySolrItemList.isEmpty()) {
                    logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " found " + logEntrySolrItemList.size() + " entries");

                    String nodeId = null;
                    // process the LogEntries into Solr Items that can be persisted
                    // processing will add date aggregated, subjects allowed to read,
                    // and a unique identifier
                    for (LogEntrySolrItem solrItem : logEntrySolrItemList) {
                        if (solrItem.getPid() == null || solrItem.getPid().trim().compareTo("") == 0) {
                            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " Blank or null identifier encountered for entryId: " + solrItem.getEntryId() + ", skipping record.");
                            continue;
                        }
                        if (solrItem.getDateLogged().after(mostRecentLoggedDate)) {
                            mostRecentLoggedDate = solrItem.getDateLogged();
                        }

                        Date now = new Date();
                        Date dateAggregated = now;
                        Identifier pid = new Identifier();
                        pid.setValue(solrItem.getPid());
                        SystemMetadata systemMetadata = systemMetadataMap.get(pid);

                        // overwrite whatever the solrItem tells us here
                        // see redmine task #4099: NodeIds of Log entries may be incorrect
                        nodeId = d1NodeReference.getValue();
                        solrItem.setNodeIdentifier(nodeId);
                        solrItem.setDateAggregated(now);
                        solrItem.setDateUpdated(initializedDate);

                        /*
                         * Fill in the solrItem fields for fields that are either obtained
                         * from systemMetadata (i.e. formatId, size for a given pid) or are
                         * derived from a field in the solrItem (i.e. location names,
                         * geohash_* are derived from the ipAddress in the solrItem).
                         */
			try {
                            solrItem.updateSysmetaFields(systemMetadata);
			} catch (Exception e) {
			    e.printStackTrace();
			    logger.error("LogHarvesterTask-" + d1NodeReference.getValue()
			        + " error setting system metadata fields for log entryId: " + solrItem.getEntryId() + ", id: " + solrItem.getPid());
                            // We encountered an unrecoverable error, so skip this record and don't add it to the list of records that will be added to the event
                            // log solr index
			    continue;
			}
			try {
                            solrItem.updateLocationFields(geoIPsvc);
			} catch (Exception e) {
			    e.printStackTrace();
			    logger.error("LogHarvesterTask-" + d1NodeReference.getValue()
			        + " error setting location fields for log entryId: " + solrItem.getEntryId() + ", id: " + solrItem.getPid());
                            // We encountered an unrecoverable error, so skip this record and don't add it to the list of records that will be added to the event
                            // log solr index
			    continue;
			}
			try {
			    solrItem.setCOUNTERfields(partialWebRobotList, fullWebRobotList, readEventCache, eventsToCheck,
			        repeatVisitIntervalSeconds, webRobotIPs, doWebRobotIPcheck);
			} catch (Exception e) {
			    e.printStackTrace();
			    logger.error("LogHarvesterTask-" + d1NodeReference.getValue()
			        + " error setting COUNTER fields for log entryId: " + solrItem.getEntryId() + ", id: " + solrItem.getPid());
                            // We encountered an unrecoverable error, so skip this record and don't add it to the list of records that will be added to the event
                            // log solr index
			    continue;
			}
                        // Purge the read event cache if it grows past a specified max value, however
                        // the number of items in the cache is determined by how far away they are from
                        // the time of the last event, so we need to check the purged size to see
                    	// if the the cache needs to be bigger.
                       	if (readEventCache.size() > readEventCacheCurrentMax) {
                            logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " Purging Read Event Cache, size: " + readEventCache.size());
                            Iterator<Map.Entry<String, DateTime>> iterator = readEventCache.entrySet().iterator();
                            DateTime eventWindowStart = new DateTime(mostRecentLoggedDate).minusSeconds(repeatVisitIntervalSeconds+1);
                            while (iterator.hasNext()) {
                                Map.Entry<String, DateTime> readEvent = iterator.next();
                                if (readEvent.getValue().isBefore(eventWindowStart)) {
                                    iterator.remove();
                                }
                            }
                            logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " Read Event Cache size after purge: " + readEventCache.size());

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
                                	logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + "Adjusting readEventCache max to: " + readEventCacheCurrentMax);
                                } else {
                                	readEventCacheCurrentMax = readEventCacheMax;
                                	logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + "Can't adjust readEventCache max to greater than: " + readEventCacheMax);
                        }
                            }
                    	}

                        /* Use the Member Node identifier combined with entryId for the Solr unique key. This natural key should be 
                         * globally unique, but also ensure that re-harvesting will not add duplicate records.
                         */
                        String id = nodeId + "." + solrItem.getEntryId();
                        solrItem.setId(id);
                        logEntrySolrItemListToPublish.add(solrItem);
                        //logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " added id: " + solrItem.getPid() + " to solt item list.");
                    }

    		    logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " Done setting fields for entries, # of items to be added: " + logEntrySolrItemListToPublish.size());

                    // publish 100 at a time, do not overwhelm the
                    // network with massive packets, or too many small packets
                    int startIndex = 0;
                    int endIndex = 0;
                    do {
                        endIndex += MAX_LIST_LOG_ENTRY_SOLR_ITEMS;
                        if (logEntrySolrItemListToPublish.size() < endIndex) {
                            endIndex = logEntrySolrItemListToPublish.size();
                        }
                        List<LogEntrySolrItem> publishEntrySolrItemList = new ArrayList<LogEntrySolrItem>(MAX_LIST_LOG_ENTRY_SOLR_ITEMS);
                        publishEntrySolrItemList.addAll(logEntrySolrItemListToPublish.subList(startIndex, endIndex));
                        logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " publishing " + publishEntrySolrItemList.size() + " items to Solr");

                        Boolean offeredLogEntry = false;
                        int offeredAttempts = 0;

                        while (!offeredLogEntry) {
                            
                            try {
                                offeredLogEntry = solrClientManager.submitBeans(publishEntrySolrItemList);
                            } catch (SolrServerException ex) {
                                logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.getMessage());
                                ex.printStackTrace();
                                offeredLogEntry = false;
                            } catch (IOException ex) {
                                logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.getMessage());
                                ex.printStackTrace();
                                offeredLogEntry = false;
                            }
                            
                            ++offeredAttempts;
                            if (offeredAttempts > MAX_OFFERED_ATTEMPTS) {
                                if (mostRecentLoggedDate.after(lastMofidiedDate)) {
                                    nodeAccess.setLogLastAggregated(d1NodeReference, mostRecentLoggedDate);
                                    logger.info("LogHarvesterTask-" + d1NodeReference.getValue() + " ServiceFailure Latested Harvested Log Entry Date " + format.format(mostRecentLoggedDate));
                                }
                                throw new ServiceFailure("-500", "attempted to offer LogEntryQueue " + MAX_OFFERED_ATTEMPTS + "try again later");
                            }
                        }
                        for (LogEntrySolrItem solrItem : publishEntrySolrItemList) {
                            if (solrItem.getDateLogged().after(mostRecentLoggedDate)) {
                                mostRecentLoggedDate = solrItem.getDateLogged();
                            }
                        }
                        try {
                            // Simple way to throttle publishing of messages
                            // thread should sleep for 250MS
                            Thread.sleep(250L);
                        } catch (InterruptedException ex) {
                            logger.warn("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.getMessage());
                        }

                        startIndex = endIndex;
                    } while (endIndex < logEntrySolrItemListToPublish.size());
                    // Persist the most recent log date in LDAP
                    if (mostRecentLoggedDate.after(lastMofidiedDate)) {
                        nodeAccess.setLogLastAggregated(d1NodeReference, mostRecentLoggedDate);
                        logger.info("LogHarvesterTask-" + d1NodeReference.getValue() + " Latested Harvested Log Entry Date " + format.format(mostRecentLoggedDate));
                    }
                }
            } while (tryAgain || (!logQueryStack.isEmpty()));
            logger.info("LogHarvesterTask-" + d1NodeReference.getValue() + " Complete");
            return mostRecentLoggedDate;
        } catch (InvalidToken ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (NotImplemented ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (InvalidRequest ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (ServiceFailure ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (EmptyStackException ex) {
            logger.warn("For some reason the date logQueryStack threw an empty exception but isEmpty reported false?");
            throw new ExecutionException(ex);
        } catch (NotAuthorized ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (NotFound ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            logger.error("LogHarvesterTask-" + d1NodeReference.getValue() + " " + ex.getMessage());
            throw new ExecutionException(ex);
        }
    }
}
