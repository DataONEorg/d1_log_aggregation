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

import com.hazelcast.core.IMap;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.batch.logging.tasks.LogHarvesterTask;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.NodeReference;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import java.text.SimpleDateFormat;
import org.dataone.cn.batch.logging.NodeHarvesterFactory;
import org.dataone.cn.batch.logging.NodeHarvester;
import org.dataone.cn.batch.logging.NodeRegistryPool;
import org.dataone.cn.ldap.NodeAccess;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.types.v2.Node;

/**
 * Quartz Job that starts off the execution of harvesting logging for a CN or a Membernode
 *
 * It executes only for a given node, and while executing excludes via a lock any other execution of a job on that
 * membernode
 *
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
        SimpleDateFormat format
                = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
        Log logger = LogFactory.getLog(LogAggregationHarvestJob.class);
        boolean nodeLocked = false;
        IMap<String, String> hzLogAggregatorLockMap = null;
        NodeReference nodeReference = new NodeReference();
        JobExecutionException jex = null;
        String nodeIdentifier = jobContext.getMergedJobDataMap().getString("NodeIdentifier");
        String lockName = nodeIdentifier;
        logger.info("Job-" + nodeIdentifier + " executing job");
        try {
            boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
            if (activateJob) {
                nodeReference.setValue(nodeIdentifier);

                // look at the node, if the boolean property of the node
                // aggregateLogs is true, then set to false
                NodeRegistryService nodeRegistryService = NodeRegistryPool.getInstance().getNodeRegistryService(nodeReference.getValue());
                NodeAccess nodeAccess = nodeRegistryService.getNodeAccess();

                if (nodeAccess.getAggregateLogs(nodeReference)) {
                    nodeAccess.setAggregateLogs(nodeReference, false);
                    Node node = nodeRegistryService.getNode(nodeReference);
                    NodeHarvester nodeHarvester = NodeHarvesterFactory.getNodeHarvester(node);

                    LogHarvesterTask harvestTask = new LogHarvesterTask(nodeHarvester);
                    Date lastProcessingCompletedDate = harvestTask.harvest();

                    if (lastProcessingCompletedDate == null) {
                        logger.info("Job-" + nodeIdentifier + " Task returned with no completion date!");
                    } else {
                        logger.info("Job-" + nodeIdentifier + " Task returned with a date of " + format.format(lastProcessingCompletedDate));
                    }
                    nodeAccess.setAggregateLogs(nodeReference, true);
                }

            } else {
                logger.error("Job-" + nodeIdentifier + " Unable to reset LDAP aggregateLogs boolean");
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Job-" + nodeIdentifier + " - died: " + ex.getMessage());
            jex = new JobExecutionException();
            jex.setStackTrace(ex.getStackTrace());
        }
        if (jex != null) {
            throw jex;
        }
    }
}
