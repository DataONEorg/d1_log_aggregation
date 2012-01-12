/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

import java.net.MalformedURLException;
import java.util.logging.Level;
import org.dataone.cn.batch.logging.tasks.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
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
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.AtomicNumber;
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
public class LogEntryQueueManager implements Callable<String> {

    Logger logger = Logger.getLogger(LogEntryQueueManager.class.getName());
    private ThreadPoolTaskExecutor taskExecutor;
    private BlockingQueue<LogEntry> indexLogEntryQueue;
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    private Integer maxIndexBufferSize = new Integer(1000);
    private String atomicNumberSequence = Settings.getConfiguration().getString("dataone.hazelcast.atomicNumberSequence");
    // Manage this class.  Init will spawn off sequential threads of this class
    // and will do so until an exception is raised.
    private HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
    private static AtomicNumber hzAtomicNumber;
    private String solrUrl = Settings.getConfiguration().getString("LogAggregator.solrUrl");
    CommonsHttpSolrServer solrServer = null;
    public void init() {


        LogEntryQueueManager logEntryQueueManager = new LogEntryQueueManager();
        hzAtomicNumber = hazelcast.getAtomicNumber(atomicNumberSequence);
        boolean shouldContinueRunning = true;
        try {
            solrServer = new CommonsHttpSolrServer(solrUrl);
            solrServer.setSoTimeout(10000);  // socket read timeout
            solrServer.setConnectionTimeout(10000);
            solrServer.setMaxRetries(1);

        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex.getMessage());
        }


        do {
            logger.debug("Starting SyncObjectExecutor");
             FutureTask futureTask = new FutureTask(logEntryQueueManager);
             taskExecutor.execute(futureTask);
            try {
                futureTask.get();
            } catch (InterruptedException ex) {
                logger.warn( ex.getMessage());
            } catch (ExecutionException ex) {
                shouldContinueRunning = false;
            } catch (Exception ex) {
                ex.printStackTrace();
                shouldContinueRunning = false;
            }
            if (futureTask.isCancelled()) {
                shouldContinueRunning = false;
            }
        } while (shouldContinueRunning);
        taskExecutor.shutdown();

    }

    @Override
    public String call() {

        logger.info("Starting LogEntryIndexTask");

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
                indexLogTask = indexLogEntryQueue.poll(30L, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // XXX this causes a nasty infinite loop of continuous failures.
                // if poll causes an exception...
                // probably should check for TIMEOUT exceptions
                // and any other causes this thread to die
                //
                indexLogTask = null;
                logger.warn(ex.getMessage());
            }
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

                if (indexLogTask == null && !(logEntryBuffer.isEmpty())) {
                    // either an exception happend or a timeout occurred.
                    // either way process the buffer of tasks
                    executeLogIndexTask(futuresMap, logEntryBuffer);
                } else {
                    logger.info("found indexLogTask " + indexLogTask.getNodeIdentifier().getValue() + ":" + indexLogTask.getEntryId() + ":" + format.format(indexLogTask.getDateLogged()) + ":" + indexLogTask.getSubject() + ":" + indexLogTask.getEvent());
                    LogEntrySolrItem solrItem = new LogEntrySolrItem(indexLogTask);
                    Date now = new Date();
                    solrItem.setDateAggregated(now);
                    Long integral = new Long(now.getTime());
                    Long decimal = new Long(hzAtomicNumber.incrementAndGet());
                    Double id = Double.parseDouble(integral.toString() + "." + decimal.toString());
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
            LogEntryIndexTask logEntryIndexTask = new LogEntryIndexTask(solrServer, indexLogEntryBuffer);
            FutureTask futureTask = new FutureTask(logEntryIndexTask);
            taskExecutor.execute(futureTask);
            futuresMap.put(futureTask,indexLogEntryBuffer);
            logEntryBuffer.clear();
        }
    }
    public BlockingQueue<LogEntry> getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue<LogEntry> indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

    public Integer getMaxIndexBufferSize() {
        return maxIndexBufferSize;
    }

    public void setMaxIndexBufferSize(Integer maxIndexBufferSize) {
        maxIndexBufferSize = maxIndexBufferSize;
    }


}
