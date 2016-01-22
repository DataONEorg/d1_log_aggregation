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
package org.dataone.cn.batch.logging;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.codehaus.plexus.util.CollectionUtils;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.batch.logging.exceptions.ScheduleManagerException;
import org.dataone.cn.batch.logging.jobs.LogAggregationHarvestJob;
import org.dataone.cn.batch.logging.jobs.LogAggregrationManageScheduleJob;
import org.dataone.cn.batch.logging.listener.SystemMetadataEntryListener;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v2.Node;
import org.dataone.service.util.DateTimeMarshaller;
import org.dataone.solr.client.solrj.impl.CommonsHttpClientProtocolRegistry;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.types.v2.NodeList;
import org.jibx.runtime.UnrecoverableException;
import org.quartz.JobExecutionContext;

/**
 * The bean must be managed by Spring. upon startup of spring it will execute via init method
 *
 * Evaluate whether the NodeList contains nodes that should be harvested for logs. It will add or remove triggers for
 * jobs based on a daily refactoring
 *
 *
 * @author waltz
 */
public class LogAggregationScheduleManager implements ApplicationContextAware {
    
    private static final int SCHEDULE_MANAGER_ONLY_RUNNING_JOB_COUNT = 100;
    private String clientCertificateLocation = Settings.getConfiguration().getString(
            "D1Client.certificate.directory")
            + File.separator
            + Settings.getConfiguration().getString("D1Client.certificate.filename");
    
    List<String> cnNodeIds = Settings.getConfiguration().getList("cn.nodeIds");
    
    private String localhostCNURL = Settings.getConfiguration().getString("D1Client.CN_URL");
    
    Logger logger = Logger.getLogger(LogAggregationScheduleManager.class.getName());
    // Quartz GroupNames for Jobs and Triggers, should be unique for a set of jobs that are related
    private static String logGroupName = "LogAggregatorHarvesting";
    
    NodeRegistryService nodeRegistryService = new NodeRegistryService();
    
    private static Scheduler scheduler;
    ApplicationContext applicationContext;
    
    private SystemMetadataEntryListener systemMetadataEntryListener;
    
    private static SimpleScheduleBuilder simpleTriggerSchedule = null;
    private static SimpleScheduleBuilder recoveryTriggerSchedule = simpleSchedule()
            .withRepeatCount(0).withMisfireHandlingInstructionFireNow();
    static final DateTimeFormatter zFmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final Date initializedDate = DateTimeMarshaller
            .deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
    static final String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");
    // Amount of time to delay the start of all jobs at initialization
    // so that not all jobs start at once, they should be staggered
    static final int delayStartOffset = Settings.getConfiguration().getInt(
            "LogAggregator.delayStartOffset.minutes", 1);
    private static final String hzNodesName = Settings.getConfiguration().getString(
            "dataone.hazelcast.nodes");
    
    private static LogAggregationScheduleManager instance;
    
    private static List<NodeReference> nodeJobsQuartzScheduled = new ArrayList<NodeReference>();
    
    private static JobKey jobScheduleManageHarvestKey = null;

    /**
     * Called by Spring to bootstrap log aggregation it will set up default intervals between job executions for
     * Membernode harvesting it will initialize Quartz it will schedule membernodes for harvesting
     *
     * it also adds a listener for changes in the hazelcast Nodes map and hz partitioner Change in hzNodes or migration
     * of partitions may entail rebalancing of quartz jobs
     *
     */
    public void init() {
        try {
            instance = this;
            // this will initialize the https protocol of the solrserver client
            // to read and send the x509 certificate
            try {
                CommonsHttpClientProtocolRegistry.createInstance();
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage());
            }
            // log aggregregation should ideally execute at least once per day per membernode
            // Sets the Period of time between sequential job executions, 24 hrs is default
            int triggerIntervalPeriod = Settings.getConfiguration().getInt(
                    "LogAggregator.triggerInterval.period", 24);
            String triggerIntervalPeriodField = Settings.getConfiguration().getString(
                    "LogAggregator.triggerInterval.periodField", "default");
            if (triggerIntervalPeriodField.equalsIgnoreCase("seconds")) {
                simpleTriggerSchedule = simpleSchedule()
                        .withIntervalInSeconds(triggerIntervalPeriod).repeatForever()
                        .withMisfireHandlingInstructionIgnoreMisfires();
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("minutes")) {
                simpleTriggerSchedule = simpleSchedule()
                        .withIntervalInMinutes(triggerIntervalPeriod).repeatForever()
                        .withMisfireHandlingInstructionIgnoreMisfires();
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("hours")) {
                simpleTriggerSchedule = simpleSchedule().withIntervalInHours(triggerIntervalPeriod)
                        .repeatForever().withMisfireHandlingInstructionIgnoreMisfires();
            } else {
                simpleSchedule().withIntervalInHours(24).repeatForever()
                        .withMisfireHandlingInstructionIgnoreMisfires();
            }
            logger.info("LogAggregationScheduler starting up");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
            
            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream(
                    "/org/dataone/configuration/logQuartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();
            
            systemMetadataEntryListener.start();
            this.scheduleHarvest();
            this.scheduleManageHarvest();
            scheduler.start();
            if (scheduler.isStarted()) {
                logger.info("Scheduler is started");
            }
        } catch (IOException ex) {
            throw new IllegalStateException(
                    "Loading properties file failedUnable to initialize jobs for scheduling: "
                    + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: "
                    + ex.getMessage());
        } catch (ServiceFailure ex) {
            throw new IllegalStateException("NodeService failed: " + ex.getMessage());
        } catch (NotImplemented ex) {
            throw new IllegalStateException("NodeService failed: " + ex.getMessage());
        }
    }

    /**
     * Create a Quartz Job that will re-evaluate the nodes scheduled for harvest once a day.
     *
     */
    private void scheduleManageHarvest() throws SchedulerException {

        //
        // start this job a day in the future at 2am
        // avoid any odd DST problems by adding a couple hours to what should be midnight
        // also avoids the problem of startTime within milliseconds of 11:59 and the job executing
        // at the same time that the schedule is already running manageHarvest through init
        //
        DateTime startTime = new DateTime().withTimeAtStartOfDay().plusDays(1).plusHours(2);
        
        jobScheduleManageHarvestKey = new JobKey("job-ScheduleManageHarvest", logGroupName);
        JobDetail job = newJob(LogAggregrationManageScheduleJob.class).withIdentity(jobScheduleManageHarvestKey).build();
        TriggerKey triggerKey = new TriggerKey("trigger-ScheduleManageHarvest",
                logGroupName);
        Trigger trigger = newTrigger().withIdentity(triggerKey).startAt(startTime.toDate())
                .withSchedule(simpleTriggerSchedule).build();
        logger.info("scheduling job-ScheduleManageHarvest to start at "
                + zFmt.print(startTime.toDate().getTime()));
        scheduler.scheduleJob(job, trigger);
        
    }

    /**
     * will perform the initial calculation or the recalculation of the scheduler.
     *
     * if scheduler is running, it will be disabled The jobs to schedule are determined by Collection math based on
     * which if any jobs have already been scheduled and the state of the nodes from listNodes
     *
     * scheduleHarvest is called by a quartz job, there is a possibility of concurrent execution of the method,
     * therefore the call is synchronized
     *
     */
    public synchronized void scheduleHarvest() throws SchedulerException, NotImplemented, ServiceFailure {
        List<NodeReference> scheduleNodes = new ArrayList<NodeReference>();
        List<NodeReference> jobsToSchedule = null;
        List<NodeReference> jobsToDelete = null;
        DateTime startTime = new DateTime();
        // delay the startTime to allow all processing to startup and
        // come to some kind of steady state... might not be possible
        // to predict, but it should be minimally 5-10 minutes
        startTime = startTime.plusMinutes(delayStartOffset);
        // halt all operations
        logger.info("manageHarvest");
        try {
            if (scheduler.isStarted()) {

            // wait until the current job (ScheduleManageHarvest)
                // is the only job executing
                waitForScheduleManagerOnlyRunningJob();
            // prevent any other jobs from being triggered
                // once the scheduler is in standby and no jobs are running, then 
                // the jobs schedule may be re-evaluated and altered
                scheduler.standby();
                logger.info("scheduler standby");
            // however the above is a race condition between this thread
                // and the scheduduler. the scheduler may have started a second job
                // before the standby call was finished executing
                // so check again before adding or deleting any jobs
                waitForScheduleManagerOnlyRunningJob();
            }
            
            NodeList nodeList = nodeRegistryService.listNodes();
            for (Node node : nodeList.getNodeList()) {
                scheduleNodes.add(node.getIdentifier());
            }

            // determine the collection of node entries to add
            jobsToSchedule = (List<NodeReference>) CollectionUtils.subtract(scheduleNodes, nodeJobsQuartzScheduled);
            // determine the collection of node entries to delete
            jobsToDelete = (List<NodeReference>) CollectionUtils.subtract(nodeJobsQuartzScheduled, scheduleNodes);
            
            logger.debug("Node map has " + nodeList.getNodeList().size() + " entries");
            logger.debug(jobsToSchedule.size() + " Jobs to Schedule");
            logger.debug(jobsToDelete.size() + " Jobs to Delete");
            if (!jobsToDelete.isEmpty()) {
                for (NodeReference nodeReference : jobsToDelete) {
                    JobKey jobKey = constructHarvestJobKey(nodeReference);
                    scheduler.deleteJob(jobKey);
                    nodeJobsQuartzScheduled.remove(nodeReference);
                }
            }
            for (Node node : nodeList.getNodeList()) {
                if (node.getState().equals(NodeState.UP)) {
                    NodeReference nodeReference = node.getIdentifier();
                    if (jobsToSchedule.contains(nodeReference)) {
                        startTime = startTime.plusSeconds(90);
                        addHarvest(nodeReference, node, startTime.toDate());
                        // cache the jobs that have been added
                        nodeJobsQuartzScheduled.add(nodeReference);
                    }
                }
            }
        } catch (ScheduleManagerException e) {
            logger.warn("Timeout from waiting for active jobs to end. Try again later!");
        }
        if (scheduler.isInStandbyMode()) {
            scheduler.start();
        }
        if (scheduler.isStarted()) {
            logger.info("Scheduler is started");
        }
        
    }

    /**
     * Create the specific Trigger and Job that should be executed by Quartz only if the MN is UP and available for
     * synchronization
     *
     * @param NodeReference
     * @param Node
     *
     */
    private void addHarvest(NodeReference nodeReference, Node node, Date startDate) {
        if (node.getState().equals(NodeState.UP)) {
            //
            // the nodelist has the router included. 
            // the router is not harvestable since it is merely a pointer
            // to CN VM. Therefore, exclude an CNs that are not listed
            // in the node.properties file as being a usable cn.nodeId
            // 
            if (node.getType().equals(NodeType.CN)
                    && !cnNodeIds.contains(node.getIdentifier().getValue())) {
                return;
            }

            // Currently, the misfire configuration in the quartz.properties is 5 minutes
            // misfire will cause the trigger to be fired again until successful
            JobKey jobKey = constructHarvestJobKey(nodeReference);
            try {
                if (!scheduler.checkExists(jobKey)) {
                    JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity(jobKey)
                            .usingJobData("NodeIdentifier", nodeReference.getValue()).build();
                    TriggerKey triggerKey = new TriggerKey("trigger-" + nodeReference.getValue(),
                            logGroupName);
                    Trigger trigger = newTrigger().withIdentity(triggerKey).startAt(startDate)
                            .withSchedule(simpleTriggerSchedule).build();
                    logger.info("scheduling job-" + nodeReference.getValue() + " to start at "
                            + zFmt.print(startDate.getTime()));
                    scheduler.scheduleJob(job, trigger);
                } else {
                    logger.error("job-" + nodeReference.getValue() + " exists!");
                }
            } catch (SchedulerException ex) {
                logger.error("Unable to initialize job key " + nodeReference.getValue()
                        + " for daily scheduling: ", ex);
            }
            
        }
    }
    
    private void waitForScheduleManagerOnlyRunningJob() throws SchedulerException, ScheduleManagerException {
        int loopCounter = 0;
        while (!(scheduler.getCurrentlyExecutingJobs().isEmpty())) {
            if (loopCounter > SCHEDULE_MANAGER_ONLY_RUNNING_JOB_COUNT) {
                throw new ScheduleManagerException();
            }
            logger.debug("Scheduler running " + scheduler.getCurrentlyExecutingJobs().size() + " jobs");
            if (scheduler.getCurrentlyExecutingJobs().size() == 1) {
                JobExecutionContext jec = scheduler.getCurrentlyExecutingJobs().get(0);
                if (jec.getJobDetail().getKey().equals(jobScheduleManageHarvestKey)) {
                    break;
                }
            }
            try {
                // wait 5 seconds before polling again
                Thread.sleep(5000L);
            } catch (InterruptedException ex) {
                logger.warn("Sleep interrupted. check again!");
            }
            loopCounter++;
        }
    }
    
    private JobKey constructHarvestJobKey(NodeReference nodeReference) {
        return new JobKey("job-" + nodeReference.getValue(), logGroupName);
    }
    
    public Scheduler getScheduler() {
        return scheduler;
    }
    
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    public SystemMetadataEntryListener getSystemMetadataEntryListener() {
        return systemMetadataEntryListener;
    }
    
    public void setSystemMetadataEntryListener(
            SystemMetadataEntryListener systemMetadataEntryListener) {
        this.systemMetadataEntryListener = systemMetadataEntryListener;
    }
    
    public static LogAggregationScheduleManager getInstance() throws Exception {
        if (instance == null) {
            throw new UnrecoverableException("LogAggregationScheduleManager is uninitialized");
        }
        return instance;
    }
    
}
