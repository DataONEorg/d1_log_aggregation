/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;
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

    private BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue;
    private org.springframework.core.io.Resource logEntryItemResource;

    private HazelcastInstance hzInstance;
    @Resource
    public void setIndexLogEntryQueue(BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue) {
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
                    LogEntrySolrItem solrItem = new LogEntrySolrItem(logEntryItem);
                    Date now = new Date();
                    solrItem.setDateAggregated(now);
                    Long integral = new Long(now.getTime());
                    Long decimal = new Long(10000L);
                    String id = integral.toString() + "." + decimal.toString();
                    solrItem.setId(id);
            List<LogEntrySolrItem> logEntryItemList = new ArrayList<LogEntrySolrItem>();
            logEntryItemList.add(solrItem);
            ITopic<List<LogEntrySolrItem>> topic = hzInstance.getTopic(Settings.getConfiguration().getString("dataone.hazelcast.logEntryTopic"));
            topic.publish(logEntryItemList);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Test
    public void getLogEntryItem() {
        List<LogEntrySolrItem> logEntryList = null;
        try {
            logEntryList = indexLogEntryQueue.poll(500L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        assertNotNull(logEntryList);
    }
}
