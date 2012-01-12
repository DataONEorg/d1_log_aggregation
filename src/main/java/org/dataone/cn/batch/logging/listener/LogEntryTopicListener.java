/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging.listener;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MessageListener;
import org.dataone.service.types.v1.LogEntry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;
/**
 *
 * @author waltz
 * 
 */
public class LogEntryTopicListener implements MessageListener<LogEntry> {

    private HazelcastInstance hazelcast;
    private BlockingQueue<LogEntry> indexLogEntryQueue;
    Logger logger = Logger.getLogger(LogEntryTopicListener.class.getName());
        SimpleDateFormat format =
                new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");
    public LogEntryTopicListener() {
        this.hazelcast = Hazelcast.getDefaultInstance();
    }

    public void init() {
        ITopic topic = Hazelcast.getTopic(hzLogEntryTopicName);
        topic.addMessageListener(this);
    }
    
    @Override
    public void onMessage(LogEntry e) {
        try {
            indexLogEntryQueue.offer(e, 30L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.error(e.getNodeIdentifier().getValue() + ":" + e.getEntryId() + ":" + format.format(e.getDateLogged()) + ":" + e.getSubject() + ":" + e.getEvent() + "--" + ex.getMessage());
        }
    }

    public BlockingQueue<LogEntry> getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue<LogEntry> indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

}
