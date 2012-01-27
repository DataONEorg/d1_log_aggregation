/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.dataone.service.types.v1.LogEntry;
import java.text.SimpleDateFormat;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.AtomicNumber;
import org.apache.solr.client.solrj.SolrServer;
import org.dataone.configuration.Settings;

/**
 * Reads from the LogEvent tasks that need to be indexed.
 *
 * Keeps track of the number of tasks that have been published
 * by use on an internal queue. When the queue is 'full' or after an
 * established wait period, create a task that will deliver the contents of the queue to the index
 *
 * It runs as a daemon thread by itself, and
 * is run as an eternal loop unless an exception is thrown.
 *
 *
 * @author waltz
 */
public class LogEntryQueueTask implements Callable {

    Logger logger = Logger.getLogger(LogEntryQueueTask.class.getName());
    // LogEntryQueueTask is itself a thread that must manage threads that
    // will actually perform the indexing
    private ThreadPoolTaskExecutor taskExecutor;
    // The BlockingQueue indexLogEntryQueue is a threadsafe, non-distributed queue shared with LogEntryQueueTask
    // It is injected via Spring
    private BlockingQueue<LogEntry> indexLogEntryQueue;
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    private Integer maxIndexBufferSize = new Integer(1000);
    private String atomicNumberSequence = Settings.getConfiguration().getString("dataone.hazelcast.atomicNumberSequence");
    // Manage this class.  Init will spawn off sequential threads of this class
    // and will do so until an exception is raised.
    private static AtomicNumber hzAtomicNumber;
    private HazelcastInstance hazelcast;
    SolrServer httpSolrServer = null;
    long pollingQueueTimeout = 60L;
    
    public void init() {

        hzAtomicNumber = hazelcast.getAtomicNumber(atomicNumberSequence);

//        try {
//             CommonsHttpSolrServer httpsolrServer = new CommonsHttpSolrServer(solrUrl);
//            httpsolrServer.setSoTimeout(30000);  // socket read timeout
//            httpsolrServer.setConnectionTimeout(30000);
//            httpsolrServer.setMaxRetries(1);
//    File home = new File( "/path/to/solr/home" );
//    File f = new File( home, "solr.xml" );
//    CoreContainer container = new CoreContainer();
//    container.load( "/path/to/solr/home", f );

//    EmbeddedSolrServer server = new EmbeddedSolrServer( container, "core name as defined in solr.xml" );
//        solrServer = httpsolrServer;
//        } catch (MalformedURLException ex) {
//            throw new RuntimeException(ex.getMessage());
//        }

    }

    @Override
    public String call() {

        logger.info("Starting LogEntryQueueTask");

        LogEntry indexLogTask = null;
        List<LogEntrySolrItem> logEntryBuffer = new ArrayList<LogEntrySolrItem>();
        // the futures map is helpful in tracking the state of a LogEntryIndexTask
        // nodecomm is need in order determine how long the comm channels have been running
        // and unavailable for use by any other task.
        // If the task has been running for over an hour, it is considered blocking and will
        // be terminated.  Once the task is terminated, the membernode will need to be
        // informed of the synchronization failure. Hence, the
        // futures map will also hold the SyncObject submitted to the TransferObjectTask

        Map<Future, List<LogEntrySolrItem>> futuresMap = new HashMap<Future, List<LogEntrySolrItem>>();
        do {
            try {
                indexLogTask = indexLogEntryQueue.poll(pollingQueueTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // XXX this causes a nasty infinite loop of continuous failures.
                // if poll causes an exception...
                // probably should check for TIMEOUT exceptions
                // and any other causes this thread to die
                //
                indexLogTask = null;
                logger.warn(ex.getMessage());
            }
            logger.info("Polled");
            // first check all the futures of past tasks to see if any have finished
            // if something timesout after X # of tries, then cancell and try again
            if (!futuresMap.isEmpty()) {
                ArrayList<Future> removalList = new ArrayList<Future>();

                for (Future future : futuresMap.keySet()) {
                    logger.info("trying future " + future.toString());
                    try {
                        future.get(500L, TimeUnit.MILLISECONDS);
                        // the future is now, reset the state of the NodeCommunication object
                        // so that it will be re-used
                        logger.debug("futureMap is done? " + future.isDone());

                        removalList.add(future);
                    } catch (CancellationException ex) {

                        logger.info("The Future has been cancelled");


                        removalList.add(future);
                    } catch (TimeoutException ex) {

                        logger.info("Waiting for the future");

                    } catch (Exception ex) {
                        logger.error(ex.getMessage());
                        ex.printStackTrace();
                        removalList.add(future);
                    }
                }
                if (!removalList.isEmpty()) {
                    for (Future key : removalList) {
                        futuresMap.remove(key);
                    }
                }
            }
            // Do not exceed the max number of tasks

            if (indexLogTask == null) {
                // either an exception happend or a timeout occurred.
                // either way process the buffer of tasks
                if (!logEntryBuffer.isEmpty()) {
                    executeLogIndexTask(futuresMap, logEntryBuffer);
                }
            } else {
                logger.debug("found indexLogTask " + indexLogTask.getNodeIdentifier().getValue() + ":" + indexLogTask.getEntryId() + ":" + format.format(indexLogTask.getDateLogged()) + ":" + indexLogTask.getSubject().getValue() + ":" + indexLogTask.getEvent());
                LogEntrySolrItem solrItem = new LogEntrySolrItem(indexLogTask);
                Date now = new Date();
                solrItem.setDateAggregated(now);
                Long integral = new Long(now.getTime());
                Long decimal = new Long(hzAtomicNumber.incrementAndGet());
                String id = integral.toString() + "." + decimal.toString();
                solrItem.setId(id);
                logEntryBuffer.add(solrItem);
                if (logEntryBuffer.size() >= maxIndexBufferSize) {
                    executeLogIndexTask(futuresMap, logEntryBuffer);
                }
            }

            logger.info("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
            if ((taskExecutor.getPoolSize() + 5) > taskExecutor.getMaxPoolSize()) {
                if ((taskExecutor.getPoolSize() == taskExecutor.getMaxPoolSize()) && futuresMap.isEmpty()) {
                    BlockingQueue<Runnable> blockingTaskQueue = taskExecutor.getThreadPoolExecutor().getQueue();
                    Runnable[] taskArray = {};
                    taskArray = blockingTaskQueue.toArray(taskArray);
                    for (int j = 0; j < taskArray.length; ++j) {
                        taskExecutor.getThreadPoolExecutor().remove(taskArray[j]);
                    }
                }
                taskExecutor.getThreadPoolExecutor().purge();
            }
        } while (true);

    }

    private void executeLogIndexTask(Map<Future, List<LogEntrySolrItem>> futuresMap, List<LogEntrySolrItem> logEntryBuffer) {
        // Do not exceed the max number of tasks
        if ((taskExecutor.getPoolSize() + 1) < taskExecutor.getMaxPoolSize()) {
            List<LogEntrySolrItem> indexLogEntryBuffer = new ArrayList<LogEntrySolrItem>();
            indexLogEntryBuffer.addAll(logEntryBuffer);
            LogEntryIndexTask logEntryIndexTask = new LogEntryIndexTask(httpSolrServer, indexLogEntryBuffer);
            FutureTask futureTask = new FutureTask(logEntryIndexTask);
            taskExecutor.execute(futureTask);
            futuresMap.put(futureTask, indexLogEntryBuffer);
            logEntryBuffer.clear();
        }
    }

    public BlockingQueue getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

    public Integer getMaxIndexBufferSize() {
        return maxIndexBufferSize;
    }

    public void setMaxIndexBufferSize(Integer maxIndexBufferSize) {
        this.maxIndexBufferSize = maxIndexBufferSize;
    }

    public ThreadPoolTaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public SolrServer getHttpSolrServer() {
        return httpSolrServer;
    }

    public void setHttpSolrServer(SolrServer httpSolrServer) {
        this.httpSolrServer = httpSolrServer;
    }

    public long getPollingQueueTimeout() {
        return pollingQueueTimeout;
    }

    public void setPollingQueueTimeout(long pollingQueueTimeout) {
        this.pollingQueueTimeout = pollingQueueTimeout;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        // shutdown the taskExecutor if this object is garbage collected
        // taskExecutor should only be issuing daemon threads
        // so no need to worry if process dies without garbage collecting
        taskExecutor.shutdown();
    }
}
