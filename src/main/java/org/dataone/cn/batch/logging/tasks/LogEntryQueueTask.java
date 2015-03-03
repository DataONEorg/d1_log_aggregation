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
package org.dataone.cn.batch.logging.tasks;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.configuration.Settings;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.hazelcast.core.HazelcastInstance;

/**
 * Reads from the LogEvent tasks that need to be indexed.
 *
 * Keeps track of the number of tasks that have been published by use on an internal queue. When the queue is 'full' or
 * after an established wait period, create a task that will deliver the contents of the queue to the index
 *
 * It runs as a daemon thread by itself, and is run as an eternal loop unless an exception is thrown.
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
    private BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue;
    SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    private Integer maxIndexBufferSize = new Integer(1000);
    // Manage this class.  Init will spawn off sequential threads of this class
    // and will do so until an exception is raised.
    private HazelcastInstance hazelcast;
    SolrServer localhostSolrServer = null;
    long pollingQueueTimeout = 60L;

    public void init() {
        logger.info("Initializing LogEntryQueueTask");
    }

    @Override
    public String call() throws Exception {
        try {
            logger.info("Starting LogEntryQueueTask");

            List<LogEntrySolrItem> indexLogTasks = null;
            List<LogEntrySolrItem> logEntryBuffer = new ArrayList<LogEntrySolrItem>();
            // the futures map is helpful in tracking the state of a LogEntryIndexTask

            Map<Future, List<LogEntrySolrItem>> futuresMap = new HashMap<Future, List<LogEntrySolrItem>>();
            do {
                boolean activateJob  = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
                if (activateJob) {
                    indexLogTasks = indexLogEntryQueue.poll(pollingQueueTimeout, TimeUnit.SECONDS);
                } else {
                    // do not listen to Sync Object queue, just finish up with any active tasks
                    // clearning out the futures map
                    // to allow for eventual shutdown
                    if (logEntryBuffer.isEmpty()) {
                        // ok futures Map is empty, no need to keep spinning here, shut this thread down
                        logger.info("All Log Entries are processed. Shutting down\n");
                        throw new ExecutionDisabledException();
                    }
                    Thread.sleep(10000L); // ten seconds
                }
                logger.debug("Polled");
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

                if (indexLogTasks == null) {
                    // either an exception happend or a timeout occurred.
                    // either way process the buffer of tasks
                    if (!logEntryBuffer.isEmpty()) {
                        executeLogIndexTask(futuresMap, logEntryBuffer);
                    }
                } else {
                    for (LogEntrySolrItem indexLogTask : indexLogTasks) {
                        logger.info("LogEntryQueueTask-" + indexLogTask.getNodeIdentifier() + ":" + indexLogTask.getEntryId() + ":" + format.format(indexLogTask.getDateLogged()) + ":" + indexLogTask.getSubject() + ":" + indexLogTask.getEvent());
                    }
                    logEntryBuffer.addAll(indexLogTasks);
                    if (logEntryBuffer.size() >= maxIndexBufferSize) {
                        executeLogIndexTask(futuresMap, logEntryBuffer);
                    }
                }

                logger.debug("ActiveCount: " + taskExecutor.getActiveCount() + " Pool size " + taskExecutor.getPoolSize() + " Max Pool Size " + taskExecutor.getMaxPoolSize());
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
        } catch (InterruptedException ex) {
            // this causes a nasty infinite loop of continuous failures.
            // if poll causes an exception...
            // probably should check for TIMEOUT exceptions
            // and any other causes this thread to die
            //

            logger.warn(ex.getMessage());
        }
        return "Completed";
    }

    private void executeLogIndexTask(Map<Future, List<LogEntrySolrItem>> futuresMap, List<LogEntrySolrItem> logEntryBuffer) {
        // Do not exceed the max number of tasks
        if ((taskExecutor.getPoolSize() + 1) < taskExecutor.getMaxPoolSize()) {
            List<LogEntrySolrItem> indexLogEntryBuffer = new ArrayList<LogEntrySolrItem>();
            indexLogEntryBuffer.addAll(logEntryBuffer);
            LogEntryIndexTask logEntryIndexTask = new LogEntryIndexTask(localhostSolrServer, indexLogEntryBuffer);
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

    public SolrServer getLocalhostSolrServer() {
        return localhostSolrServer;
    }

    public void setLocalhostSolrServer(SolrServer localhostSolrServer) {
        this.localhostSolrServer = localhostSolrServer;
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
