/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.tasks;

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
import org.joda.time.MutableDateTime;

/**
 * An executable task that retrieve a list of ObjectInfos
 * by calling listObject on a MN  and then submits them on
 * the SyncTaskQueue for processing. It will retrieve and submit
 * in batches of 1000 as the default.
 *
 * As an executable, it will return a date that is the latest DateSysMetadataModified
 * of the processed nodelist
 * 
 * @author waltz
 */
public class LogAggregatorTask implements Callable<Date>, Serializable {

    NodeReference d1NodeReference;
    private Session session;
    private int start = 0;
    private int total = 0;
    Integer batchSize;
    private Date now = new Date();
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    public LogAggregatorTask(NodeReference d1NodeReference, Integer batchSize) {
        this.d1NodeReference = d1NodeReference;
        this.batchSize = batchSize;
    }

    @Override
    public Date call() throws Exception {
        // we are going to write directly to ldap for the updateLastHarvested
        // because we do not want hazelcast to spam us about
        // all of these updates since we have a listener in HarvestSchedulingManager
        // that determines when updates/additions have occured and 
        // re-adjusts scheduling
        NodeRegistryService nodeRegistryService = new NodeRegistryService();
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        logger.debug("called ObjectListHarvestTask");
        HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
        ITopic<LogEntry> hzLogEntryTopic = hazelcast.getTopic(hzLogEntryTopicName);
        // Need the LinkedHashMap to preserver insertion order
        Node d1Node = nodeRegistryService.getNode(d1NodeReference);
        Date lastMofidiedDate = nodeRegistryService.getLogLastAggregated(d1NodeReference);
        if (lastMofidiedDate == null) {
            lastMofidiedDate = DateTimeMarshaller.deserializeDateToUTC("1900-00-00T00:00:00.000-00:00");
        }
        List<LogEntry> readQueue = null;
        String d1NodeBaseUrl = d1Node.getBaseURL();
        if (d1Node.getType().equals(NodeType.CN)) {
            d1NodeBaseUrl =  Settings.getConfiguration().getString("LogAggregator.cn_base_url");
        }
        do {
            // read upto a 1000 objects (the default, but it can be overwritten)
            // from ListObjects and process before retrieving more
            if (start == 0 || (start < total)) {
                readQueue = this.retrieve(d1NodeBaseUrl, lastMofidiedDate);

                for (LogEntry logEntry : readQueue) {
                    hzLogEntryTopic.publish(logEntry);
                    // Simple way to throttle publishing of messages
                    // thread should sleep of 250MS
                    Thread.sleep(250L);
                }
            } else {
                readQueue = null;
            }
        } while ((readQueue != null) && (!readQueue.isEmpty()));
        

        nodeRegistryService.setLogLastAggregated(d1NodeReference, now);

        // return the date of completion of the task
        return new Date();
    }

    /*
     * performs the retrieval of the nodelist from a membernode.
     * It retrieves the list in batches and should be called iteratively
     * until all objects have been retrieved from a node.
     */
    private List<LogEntry> retrieve(String mnUrl, Date fromDate) {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());

        MNode mNode = new MNode(mnUrl);

        List<LogEntry> writeQueue = new ArrayList<LogEntry>();

        Date startTime;
        Log logList = null;
        Boolean replicationStatus = null;

        MutableDateTime lastHarvestDateTime = new MutableDateTime( fromDate);

        lastHarvestDateTime.addMillis(1);
        Date lastHarvestDate = lastHarvestDateTime.toDate();
        
        logger.debug("starting retrieval " + mnUrl);
        try {

            // always execute for the first run (for start = 0)
            // otherwise skip because when the start is equal or greater
            // then total, then all objects have been harvested

            logList = mNode.getLogRecords(session, lastHarvestDate, now, null, start, batchSize);
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
