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
package org.dataone.cn.batch.logging.listener;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.dataone.service.types.v1.LogEntry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.log4j.Logger;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.configuration.Settings;

/**
 *
 * @author waltz
 *
 */
public class LogEntryTopicListener implements MessageListener<List<LogEntrySolrItem>> {

    private HazelcastInstance hazelcast;
    // The BlockingQueue indexLogEntryQueue is a threadsafe, non-distributed queue shared with LogEntryQueueTask
    // It is injected via Spring
    private BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue;
    Logger logger = Logger.getLogger(LogEntryTopicListener.class.getName());
    private static SimpleDateFormat format =
            new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    static final String hzLogEntryTopicName = Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic");

    public void addListener() {
        logger.info("Starting LogEntryTopicListener");
        ITopic topic = hazelcast.getTopic(hzLogEntryTopicName);
        topic.addMessageListener(this);
    }

    public BlockingQueue getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    @Override
    public void onMessage(Message<List<LogEntrySolrItem>> message) {
        boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
        if (activateJob) {
            
            List<LogEntrySolrItem> logList = message.getMessageObject();
            try {
                indexLogEntryQueue.offer(logList, 30L, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                for (LogEntrySolrItem e : logList) {
                    logger.error("Unable to offer " + e.getNodeIdentifier() + ":" + e.getEntryId() + ":" + format.format(e.getDateLogged()) + ":" + e.getSubject() + ":" + e.getEvent() + "--" + ex.getMessage());
                }
            }
        }
    }
}
