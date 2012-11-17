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

package org.dataone.cn.batch.logging;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
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
import org.junit.AfterClass;
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
    @AfterClass
    public static void shutdownHazelcast() {
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }
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
    public void setHzInstance(HazelcastInstance hazelcastInstance) {
        this.hzInstance = hazelcastInstance;
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
