/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging.listener;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MessageListener;
import org.dataone.service.types.v1.LogEntry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.configuration.Settings;
/**
 *
 * @author waltz
 * 
 */
public class LogEntryTopicListener implements MessageListener<LogEntrySolrItem> {

    private HazelcastInstance hazelcast;

    // The BlockingQueue indexLogEntryQueue is a threadsafe, non-distributed queue shared with LogEntryQueueTask
    // It is injected via Spring
    private BlockingQueue<LogEntrySolrItem> indexLogEntryQueue;
    Logger logger = Logger.getLogger(LogEntryTopicListener.class.getName());
    private static SimpleDateFormat format =
                new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");


    public void addListener() {
        logger.info("Starting LogEntryTopicListener");
        ITopic topic = hazelcast.getTopic(hzLogEntryTopicName);
        topic.addMessageListener(this);
    }
    
    @Override
    public void onMessage(LogEntrySolrItem e) {
        try {
            logger.debug("offering " + e.getEntryId());
            indexLogEntryQueue.offer(e, 30L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.error("Unable to offer " + e.getNodeIdentifier() + ":" + e.getEntryId() + ":" + format.format(e.getDateLogged()) + ":" + e.getSubject() + ":" + e.getEvent() + "--" + ex.getMessage());
        }
    }

    public BlockingQueue getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

    public  HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public  void setHazelcast(HazelcastInstance hazelcast) {
         this.hazelcast = hazelcast;
    }

}
