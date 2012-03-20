/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.tasks;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.dataone.client.MNode;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
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
    private int start = 0;
    private int total = 0;
    Integer batchSize;
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    private static AtomicNumber hzAtomicNumber;
    private String atomicNumberSequence = Settings.getConfiguration().getString("dataone.hazelcast.atomicNumberSequence");
    String hzSystemMetaMapString = Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    static private int triggerIntervalPeriod = Settings.getConfiguration().getInt("LogAggregator.triggerInterval.period");
    static private String triggerIntervalPeriodField = Settings.getConfiguration().getString("LogAggregator.triggerInterval.periodField");
    public LogAggregatorTask(NodeReference d1NodeReference, Integer batchSize) {
        this.d1NodeReference = d1NodeReference;
        this.batchSize = batchSize;
    }

    /**
     * Implement the Callable interface, providing code retrieves logging information from a D1 Node
     *
     * @return Date
     * @throws Exception
     * @author waltz
     */
    @Override
    public Date call() throws ExecutionException {
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        try {

            HazelcastInstance hzclient = HazelcastClientInstance.getHazelcastClient();
            // midnight of the current date is the latest date until which we wish to retrieve data
            DateTime midnight = new DateTime(DateTimeZone.UTC);
            // for testing
            //        midnight.minusMinutes(2);
            if (triggerIntervalPeriodField.equalsIgnoreCase("seconds")) {
                 // midnight is really now offset by a seconds found in config file
                midnight.minusSeconds(triggerIntervalPeriod);
                // midnight is really now offset by a minutes found in config file
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("minutes")) {
                midnight.minusMinutes(triggerIntervalPeriod);
            } else {
                // midnight is really midnight
                midnight = midnight.withTime(0, 0, 0, 0);
            }


            IMap<Identifier, SystemMetadata> systemMetadataMap = hzclient.getMap(hzSystemMetaMapString);
            Subject publicSubject = new Subject();
            publicSubject.setValue(Constants.SUBJECT_PUBLIC);
            Subject authenticatedSubject = new Subject();
            authenticatedSubject.setValue(Constants.SUBJECT_AUTHENTICATED_USER);
            Subject verifiedSubject = new Subject();
            verifiedSubject.setValue(Constants.SUBJECT_VERIFIED_USER);
            // we are going to write directly to ldap for the LogLastAggregated
            // because we do not want hazelcast to spam us about
            // all of these updates since we have a listener in HarvestSchedulingManager
            // that determines when updates/additions have occured and
            // re-adjusts scheduling
            NodeRegistryService nodeRegistryService = new NodeRegistryService();
            NodeAccess nodeAccess = new NodeAccess();
            // logger is not  be serializable, but no need to make it transient imo
            logger.debug("called LogAggregatorTask");
            HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
            ITopic<List<LogEntrySolrItem>> hzLogEntryTopic = hazelcast.getTopic(hzLogEntryTopicName);
            // Need the LinkedHashMap to preserver insertion order
            Node d1Node = nodeRegistryService.getNode(d1NodeReference);
            Date lastMofidiedDate = nodeAccess.getLogLastAggregated(d1NodeReference);
            hzAtomicNumber = hazelcast.getAtomicNumber(atomicNumberSequence);
            if (lastMofidiedDate == null) {
                lastMofidiedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
            }
            Date lastLoggedDate = new Date(lastMofidiedDate.getTime());
            List<LogEntry> readQueue = null;
            String d1NodeBaseUrl = d1Node.getBaseURL();
            if (d1Node.getType().equals(NodeType.CN)) {
                d1NodeBaseUrl = Settings.getConfiguration().getString("LogAggregator.cn_base_url");
            }
            do {
                // read upto a 1000 objects (the default, but it can be overwritten)
                // from ListObjects and process before retrieving more
                if (start == 0 || (start < total)) {
                    readQueue = this.retrieve(d1NodeBaseUrl, lastMofidiedDate, midnight.toDate());
                    logger.debug("found " + readQueue.size() + " entries");
                    List<LogEntrySolrItem> logEntrySolrItemList = new ArrayList<LogEntrySolrItem>(batchSize);
                    for (LogEntry logEntry : readQueue) {
                        if (logEntry.getDateLogged().after(lastLoggedDate)) {
                            lastLoggedDate = logEntry.getDateLogged();
                        }
                        Date now = new Date();
                        LogEntrySolrItem solrItem = new LogEntrySolrItem(logEntry);
                        solrItem.setDateAggregated(now);
                        SystemMetadata systemMetadata = systemMetadataMap.get(logEntry.getIdentifier());
                        if (systemMetadata != null) {
                            List<String> subjectsAllowedRead = new ArrayList<String>();
                            // RightsHolder always has read permission
                            // even if SystemMetadata does not have an AccessPolicy
                            Subject rightsHolder = systemMetadata.getRightsHolder();
                            if ((rightsHolder != null) && !(rightsHolder.getValue().isEmpty())) {
                                try {
                                    X500Principal principal = new X500Principal(rightsHolder.getValue());
                                    String standardizedName = principal.getName(X500Principal.RFC2253);
                                    subjectsAllowedRead.add(standardizedName);
                                } catch (IllegalArgumentException ex) {
                                    logger.warn("Found improperly formatted rights holder subject: " + rightsHolder.getValue() + "\n" + ex.getMessage());
                                }
                            }
                            if (systemMetadata.getAccessPolicy() != null) {
                                List<AccessRule> allowList = systemMetadata.getAccessPolicy().getAllowList();
                                for (AccessRule accessRule : allowList) {
                                    List<Subject> subjectList = accessRule.getSubjectList();
                                    for (Subject accessSubject : subjectList) {
                                        if (accessSubject.equals(publicSubject)) {
                                            // set Public access boolean on record, it is a shortcut
                                            solrItem.setIsPublic(true);
                                            subjectsAllowedRead.add(Constants.SUBJECT_PUBLIC);
                                        } else if (accessSubject.equals(authenticatedSubject)) {
                                            subjectsAllowedRead.add(Constants.SUBJECT_AUTHENTICATED_USER);
                                        } else if (accessSubject.equals(verifiedSubject)) {
                                            subjectsAllowedRead.add(Constants.SUBJECT_VERIFIED_USER);
                                        } else {
                                            try {
                                                // add subject as having read access on the record
                                                X500Principal principal = new X500Principal(accessSubject.getValue());
                                                String standardizedName = principal.getName(X500Principal.RFC2253);
                                                subjectsAllowedRead.add(standardizedName);
                                            } catch (IllegalArgumentException ex) {
                                                // It may be a group, or a psuedo user, so just add the subject's value
                                                // without attempting to standardize it
                                                subjectsAllowedRead.add(accessSubject.getValue());
                                                logger.warn(accessSubject.getValue() +" does not conform to RFC2253 conventions");
                                            }
                                        }
                                    }
                                }
                            } else {
                                logger.warn("SystemMetadata with pid " + logEntry.getIdentifier().getValue() + " does not have an access policy");
                            }
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
                            // thread should sleep of 250MS
                            Thread.sleep(250L);
                        } catch (InterruptedException ex) {
                            logger.warn(ex.getMessage());
                        }
                        startIndex = endIndex;
                    } while (endIndex < logEntrySolrItemList.size());
                } else {
                    readQueue = null;
                }
            } while ((readQueue != null) && (!readQueue.isEmpty()));
            if (lastLoggedDate.after(lastMofidiedDate)) {
                nodeAccess.setLogLastAggregated(d1NodeReference, lastLoggedDate);
            }
            return lastLoggedDate;
        } catch (ServiceFailure ex) {
            ex.printStackTrace();
            logger.error(ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (NotFound ex) {
            ex.printStackTrace();
            logger.error(ex.serialize(ex.FMT_XML));
            throw new ExecutionException(ex);
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            throw new ExecutionException(ex);
        }
    }

    /*
     * performs the retrieval of the log records  from a DataONE node.
     * It retrieves the list in batches and should be called iteratively
     * until all log entries have been retrieved from a node.
     * 
     * @return List<LogEntry>
     * @author waltz
     */
    private List<LogEntry> retrieve(String mnUrl, Date fromDate, Date toDate) {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());

        MNode mNode = new MNode(mnUrl);

        List<LogEntry> writeQueue = new ArrayList<LogEntry>();

        Log logList = null;

        MutableDateTime lastHarvestDateTime = new MutableDateTime(fromDate);

        lastHarvestDateTime.addMillis(1);
        fromDate = lastHarvestDateTime.toDate();

        logger.debug("starting retrieval " + mnUrl);
        try {

            // always execute for the first run (for start = 0)
            // otherwise skip because when the start is equal or greater
            // then total, then all objects have been harvested

            logList = mNode.getLogRecords(session, fromDate, toDate, null, start, batchSize);
            // if objectList is null or the count is 0 or the list is empty, then
            // there is nothing to process
            if (!((logList == null)
                    || (logList.getCount() == 0)
                    || (logList.getLogEntryList().isEmpty()))) {

                start += logList.getCount();
                writeQueue.addAll(logList.getLogEntryList());
                total = logList.getTotal();

            }
        } catch (NotAuthorized ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InvalidRequest ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (NotImplemented ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (ServiceFailure ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        } catch (InvalidToken ex) {
            logger.error(ex.serialize(ex.FMT_XML));
        }

        return writeQueue;
    }
}
