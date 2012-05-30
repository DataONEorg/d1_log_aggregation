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
package org.dataone.cn.batch.logging.jobs;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.batch.logging.tasks.LogAggregatorTask;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.NodeReference;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import java.text.SimpleDateFormat;
import org.dataone.cn.batch.exceptions.ExecutionDisabledException;
import org.dataone.cn.batch.logging.LocalhostTaskExecutorFactory;
import org.springframework.core.task.AsyncTaskExecutor;

/**
 * Quartz Job that starts off the hazelcast distributed execution of harvesting logging from a Membernode
 *
 * It executes only for a given membernode, and while executing excludes via a lock any other execution of a job on that
 * membernode
 *
 * If the node provided is the localhost coordinating node, then the task is executed locally
 *
 * Job may not be executed concurrently for a single membernode or coordinating node
 *
 * Keep track of last date harvested
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class LogAggregationHarvestJob implements Job {

    @Override
    public void execute(JobExecutionContext jobContext) throws JobExecutionException {

        // do not submit the localCNIdentifier to Hazelcast for execution
        // rather execute it locally on the machine

        SimpleDateFormat format =
                new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
        Log logger = LogFactory.getLog(LogAggregationHarvestJob.class);
        boolean nodeLocked = false;
        IMap<String, String> hzLogAggregatorLockMap = null;
        NodeReference nodeReference = new NodeReference();
        JobExecutionException jex = null;
        String nodeIdentifier = jobContext.getMergedJobDataMap().getString("NodeIdentifier");
        String lockName = nodeIdentifier;
        try {
            boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
            if (activateJob) {
                String hzLogAggregationLockMapString = Settings.getConfiguration().getString("dataone.hazelcast.logAggregatorLock");
                String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");

                nodeReference.setValue(nodeIdentifier);

                // Locking on the default instance.
                // The locking mechanism can not be on the nodeList
                // since synchronization uses locking on the nodeList
                HazelcastInstance hazelcast = Hazelcast.getDefaultInstance();
                hzLogAggregatorLockMap = hazelcast.getMap(hzLogAggregationLockMapString);
                if (hzLogAggregatorLockMap.get(lockName) == null) {
                    hzLogAggregatorLockMap.put(lockName, "1");
                }


                Integer batchSize = Settings.getConfiguration().getInt("LogAggregator.logRecords_batch_size");
                logger.info("executing for " + nodeIdentifier + " with batch size " + batchSize);
                nodeLocked = hzLogAggregatorLockMap.tryLock(lockName, 500L, TimeUnit.MILLISECONDS);
                Future future = null;
                if (nodeLocked) {

                    LogAggregatorTask harvestTask = new LogAggregatorTask(nodeReference, batchSize);
                    // If the node reference is the local machine nodId, then
                    // do not submit to hazelcast for distribution
                    // Rather, execute it on the local machine
                    if (nodeReference.getValue().equals(localCnIdentifier)) {
                        // Execute on localhost
                        AsyncTaskExecutor executor = LocalhostTaskExecutorFactory.getSimpleTaskExecutor();
                        future = executor.submit(harvestTask);
                    } else {
                        // Distribute the task to any hazelcast process cluster instance
                        DistributedTask dtask = new DistributedTask((Callable<Date>) harvestTask);
                        ExecutorService executor = hazelcast.getExecutorService();
                        future = executor.submit(dtask);
                    }
                    Date lastProcessingCompletedDate = null;
                    try {
                        lastProcessingCompletedDate = (Date) future.get();
                    } catch (InterruptedException ex) {
                        logger.error(ex.getMessage());
                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof ExecutionDisabledException) {
                            logger.error("ExecutionDisabledException: " + ex.getMessage() + "\n\tExecutionDisabledException: Will fire Job again\n");
                            jex = new JobExecutionException();
                            jex.setStackTrace(ex.getStackTrace());
                            jex.setRefireImmediately(true);
                            Thread.sleep(5000L);
                        } else {
                            logger.error("ExecutionException: " + ex.getMessage());
                        }
                    }
                    // if the lastProcessingCompletedDate has changed then it should be persisted, but where?
                    // Does not need to be stored, maybe just printed?
                    if (lastProcessingCompletedDate == null) {
                        logger.info("LogAggregatorTask returned with no completion date!");
                    } else {
                        logger.info("LogAggregatorTask returned with a date of " + format.format(lastProcessingCompletedDate));
                    }
                    // think about putting the jobContext.getFireInstanceId() on a queue
                    // or something so that all the entries for that job get submitted
                    // to lucune solr in batch
                } else {
                    // log this message, someone else has the lock (and they probably shouldn't)
                    try {
                        // sleep for 30 seconds?
                        Thread.sleep(30000L);
                        logger.warn(jobContext.getJobDetail().getDescription() + " locked");

                        jex = new JobExecutionException();
                        jex.setRefireImmediately(true);
                    } catch (InterruptedException ex) {
                        logger.debug(ex.getMessage());
                    }

                }
            }
        } catch (Exception ex) {
            logger.error(jobContext.getJobDetail().getDescription() + " died: " + ex.getMessage());
            jex = new JobExecutionException();
            jex.unscheduleFiringTrigger();
            jex.setStackTrace(ex.getStackTrace());
        } finally {
            if (nodeLocked && (hzLogAggregatorLockMap != null)) {
                hzLogAggregatorLockMap.unlock(lockName);
            }
        }
        if (jex != null) {
            throw jex;
        }
    }
}
