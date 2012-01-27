/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.List;
import org.dataone.cn.solr.client.solrj.MockSolrServer;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dataone.cn.batch.logging.tasks.LogEntryQueueTask;
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
 * Tests the functionality of LogEntryQueueTask and subsequently LogEntryIndexTask
 * since both tasks are run as threads, the tests must set them up
 * to run in separate executors.
 * After an event is queued for processing, this test should sleep
 * for longer than what the LogEntryQueueTask would take to submit
 * LogEntryIndexTask for processing.
 * The test will then check the mock implementation of the Solr Server
 * to ensure that LogEntryIndexTask added the events to be indexed.
 * 
 * @author waltz
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/configuration/logEntryQueueTaskContext.xml"})
public class LogEntryQueueTaskTestCase {

    private BlockingQueue<LogEntry> indexLogEntryQueue;
    private org.springframework.core.io.Resource logEntryItemResource;

    private HazelcastInstance hzInstance;

    private LogEntryQueueTask logEntryQueueTask;

    private ThreadPoolTaskExecutor logIndexingThreadPoolTaskExecutor;

    private MockSolrServer httpSolrServer;
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
    @Resource
    public void setLogEntryQueueTask(LogEntryQueueTask logEntryQueueTask) {
        this.logEntryQueueTask = logEntryQueueTask;
    }
    @Resource
    public void setLogIndexingThreadPoolTaskExecutor(ThreadPoolTaskExecutor logIndexingThreadPoolTaskExecutor) {
        this.logIndexingThreadPoolTaskExecutor = logIndexingThreadPoolTaskExecutor;
    }
    @Resource
    public void setHttpSolrServer(MockSolrServer httpSolrServer) {
        this.httpSolrServer = httpSolrServer;
    }
    @Before
    public void init() {
        try {
            LogEntry logEntryItem = TypeMarshaller.unmarshalTypeFromStream(LogEntry.class, logEntryItemResource.getInputStream());
            indexLogEntryQueue.offer(logEntryItem, 500L, TimeUnit.MILLISECONDS);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (InstantiationException ex) {
           ex.printStackTrace();
        } catch (IllegalAccessException ex) {
           ex.printStackTrace();
        } catch (JiBXException ex) {
           ex.printStackTrace();
        } catch (InterruptedException ex) {
           ex.printStackTrace();
        }
    }



    @Test
    public void mockIndexSolrItem () {
        // start off a single thread with LogEntryQueueTask
        // it should after a few seconds push the entry to the LogEntryIndexTask
        // LogEntryIndexTask should add the item to the MockSolrServer
        // End the LogEntryQueueTask, and see if the MockSolrServer
        // has the item
        ExecutorService executor = Executors.newSingleThreadExecutor(new DaemonFactory());
        Future future = executor.submit(new FutureTask(logEntryQueueTask));
        try {
            // sleep longer than the configured pollingQueueTimeout of the logEntryQueueTask
            Thread.sleep(2500L);
        } catch (InterruptedException ex) {
            Logger.getLogger(LogEntryQueueTaskTestCase.class.getName()).log(Level.SEVERE, null, ex);
        }
        future.cancel(false);
        executor.shutdownNow();
        List<LogEntrySolrItem> indexLogEntryBuffer = (List<LogEntrySolrItem>) httpSolrServer.getAddedBeans();
        assertNotNull(indexLogEntryBuffer);
        assertTrue(indexLogEntryBuffer.size() > 0);
    }




    private class DaemonFactory implements ThreadFactory {

        public Thread newThread(Runnable task) {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            return thread;
        }
    }

}
