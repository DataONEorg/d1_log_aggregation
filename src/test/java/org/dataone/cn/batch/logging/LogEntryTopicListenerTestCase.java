/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

import com.hazelcast.core.ITopic;
import org.dataone.configuration.Settings;
import com.hazelcast.core.HazelcastInstance;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.junit.Assert.*;

/**
 * Having a problem getting the BlockingQueue to configure correctly via Spring
 * Also, test out the LogEntrySolrItem class
 * @author waltz
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/configuration/logEntryTopicListenerContext.xml"})
public class LogEntryTopicListenerTestCase {

    private BlockingQueue<LogEntry> indexLogEntryQueue;
    private org.springframework.core.io.Resource logEntryItemResource;

    private HazelcastInstance hzInstance;
    @Resource
    public void setIndexLogEntryQueue(BlockingQueue<LogEntry> indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

    @Resource
    public void setLogEntryItemResource(org.springframework.core.io.Resource logEntryItemResource) {
        this.logEntryItemResource = logEntryItemResource;
    }
    @Resource
    public void setHzInstance(HazelcastInstance hzInstance) {
        this.hzInstance = hzInstance;
    }
    @Before
    public void initQueue() {

        try {
            LogEntry logEntryItem = TypeMarshaller.unmarshalTypeFromStream(LogEntry.class, logEntryItemResource.getInputStream());
            
            ITopic<LogEntry> topic = hzInstance.getTopic(Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic"));
            topic.publish(logEntryItem);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Test
    public void getLogEntryItem() {
        LogEntry logEntry = null;
        try {
            logEntry = indexLogEntryQueue.poll(500L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        assertNotNull(logEntry);
    }
}
