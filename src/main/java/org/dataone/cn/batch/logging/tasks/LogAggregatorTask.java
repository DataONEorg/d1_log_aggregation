/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.tasks;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import org.dataone.client.MNode;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.ldap.NodeAccess;

import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Session;
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
    public LogAggregatorTask(NodeReference d1NodeReference, Integer batchSize) {
        this.d1NodeReference = d1NodeReference;
        this.batchSize = batchSize;
    }

    @Override
    public Date call() throws Exception {

        // midnight of the current date is the latest date until which we wish to retrieve data
        DateTime midnight = new DateTime(DateTimeZone.UTC);
        // for testing
//        midnight.minusMinutes(2);
        midnight = midnight.withTime(0, 0, 0, 0);

        // we are going to write directly to ldap for the LogLastAggregated
        // because we do not want hazelcast to spam us about
        // all of these updates since we have a listener in HarvestSchedulingManager
        // that determines when updates/additions have occured and 
        // re-adjusts scheduling
        NodeRegistryService nodeRegistryService = new NodeRegistryService();
        NodeAccess nodeAccess = new NodeAccess();
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        logger.debug("called LogAggregatorTask");
        HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
        ITopic<LogEntrySolrItem> hzLogEntryTopic = hazelcast.getTopic(hzLogEntryTopicName);
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
                for (LogEntry logEntry : readQueue) {
                    if (logEntry.getDateLogged().after(lastLoggedDate)) {
                        lastLoggedDate = logEntry.getDateLogged();
                    }
                    Date now = new Date();
                    LogEntrySolrItem solrItem = new LogEntrySolrItem(logEntry);
                    solrItem.setDateAggregated(now);
                    Long integral = new Long(now.getTime());
                    Long decimal = new Long(hzAtomicNumber.incrementAndGet());
                    String id = integral.toString() + "." + decimal.toString();
                    solrItem.setId(id);
                    hzLogEntryTopic.publish(solrItem);
                    logger.debug("published " + logEntry.getEntryId());
                    // Simple way to throttle publishing of messages
                    // thread should sleep of 250MS
                    Thread.sleep(250L);

                }
            } else {
                readQueue = null;
            }
        } while ((readQueue != null) && (!readQueue.isEmpty()));

        if (lastLoggedDate.after(lastMofidiedDate)) {
            nodeAccess.setLogLastAggregated(d1NodeReference, lastLoggedDate);
        }

        // return the date of completion of the task
        return lastLoggedDate;
    }

    /*
     * performs the retrieval of the nodelist from a membernode.
     * It retrieves the list in batches and should be called iteratively
     * until all objects have been retrieved from a node.
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
