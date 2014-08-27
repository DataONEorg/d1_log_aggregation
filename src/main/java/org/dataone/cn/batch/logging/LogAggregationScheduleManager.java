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

import org.dataone.cn.batch.logging.listener.SystemMetadataEntryListener;
import org.dataone.cn.ldap.ProcessingState;
import org.dataone.cn.ldap.NodeAccess;
import org.quartz.JobDataMap;
import org.dataone.cn.batch.logging.jobs.LogAggregationRecoverJob;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.dataone.service.util.DateTimeMarshaller;

import java.util.Map;

import org.dataone.service.exceptions.ServiceFailure;
import org.quartz.SimpleScheduleBuilder;
import org.dataone.client.auth.CertificateManager;
import org.dataone.configuration.Settings;

import java.util.Date;
import java.io.File;

import com.hazelcast.partition.PartitionService;

import java.util.List;

import com.hazelcast.core.Member;

import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.Node;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import com.hazelcast.core.IMap;

import org.quartz.JobDetail;
import org.dataone.cn.batch.logging.jobs.LogAggregationHarvestJob;

import com.hazelcast.core.HazelcastInstance;

import java.net.MalformedURLException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.dataone.cn.hazelcast.HazelcastLdapStore;
import org.dataone.solr.client.solrj.impl.CommonsHttpClientProtocolRegistry;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

import org.joda.time.DateTime;
import org.quartz.*;

import static org.quartz.JobBuilder.*;

/**
 * this bean must be managed by Spring upon startup of spring it will execute via init method
 *
 * evaluate whether the NodeList contains nodes that should be harvested for logs on the executing coordinating node. It
 * will add or remove triggers for jobs based on events, such as startup, nightly refactoring, more CNs coming online
 *
 *
 * @author waltz
 */
public class LogAggregationScheduleManager implements ApplicationContextAware {

    private String clientCertificateLocation =
            Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");
    private String localhostCNURL = Settings.getConfiguration().getString("D1Client.CN_URL");
    public static Log logger = LogFactory.getLog(LogAggregationScheduleManager.class);
    // Quartz GroupNames for Jobs and Triggers, should be unique for a set of jobs that are related
    private static String logGroupName = "LogAggregatorHarvesting";
    private static String recoveryGroupName = "LogAggregatorRecovery";
    private BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue;
    
    private HazelcastInstance hazelcast;
    private HazelcastLdapStore hazelcastLdapStore;
    private Scheduler scheduler;
    ApplicationContext applicationContext;
    PartitionService partitionService;
    Member localMember;
    private SolrServer localhostSolrServer;  // Injected by Spring
    //private LogEntryTopicListener logEntryTopicListener;
    private SystemMetadataEntryListener systemMetadataEntryListener;
    
    private static SimpleScheduleBuilder harvestSimpleTriggerSchedule = null;
    // Recovery only runs once for each CN
    private static SimpleScheduleBuilder recoverySimpleTriggerSchedule = simpleSchedule().withRepeatCount(0).withMisfireHandlingInstructionFireNow();
    static final DateTimeFormatter zFmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
    static final String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");
    private String activeCnIdentifier = Settings.getConfiguration().getString("cn.nodeId.active");
    // Amount of time to delay the start of all jobs at initialization
    // so that not all jobs start at once, they should be staggered
    static final int delayStartOffset = Settings.getConfiguration().getInt("LogAggregator.delayStartOffset.minutes", 10);
    // Amount of time to delay the start the recovery job at initialization
    static final int delayRecoveryOffset = Settings.getConfiguration().getInt("LogAggregator.delayRecoveryOffset.minutes", 5);
    private static final String hzNodesName = Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    /* 
     * Called by Spring to bootstrap log aggregation
     * it will set up default intervals between job executions for Membernode harvesting
     * it will initialize Quartz
     * it will schedule membernodes for harvesting
     * 
     * it also adds a listener for changes in the hazelcast Nodes map and hz partitioner
     * Change in hzNodes or migration of partitions may entail rebalancing of quartz jobs
     * 
     */
    public void init() {
        try {
            // this will initialize the https protocol of the solrserver client
            // to read and send the x509 certificate
            try {
                CommonsHttpClientProtocolRegistry.createInstance();
            } catch (Exception ex) {
                ex.printStackTrace();
                logger.error(ex.getMessage());
            } 
            // log aggregation should ideally execute at least once per day per membernode
            // Sets the Period of time between sequential job executions, 24 hrs is default
            int triggerIntervalPeriod = Settings.getConfiguration().getInt("LogAggregator.triggerInterval.period", 24);
            String triggerIntervalPeriodField = Settings.getConfiguration().getString("LogAggregator.triggerInterval.periodField", "default");
            if (triggerIntervalPeriodField.equalsIgnoreCase("seconds")) {
            	harvestSimpleTriggerSchedule = simpleSchedule().withIntervalInSeconds(triggerIntervalPeriod).repeatForever().withMisfireHandlingInstructionIgnoreMisfires();
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("minutes")) {
            	harvestSimpleTriggerSchedule = simpleSchedule().withIntervalInMinutes(triggerIntervalPeriod).repeatForever().withMisfireHandlingInstructionIgnoreMisfires();
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("hours")) {
            	harvestSimpleTriggerSchedule = simpleSchedule().withIntervalInHours(triggerIntervalPeriod).repeatForever().withMisfireHandlingInstructionIgnoreMisfires();
            } else {
                simpleSchedule().withIntervalInHours(24).repeatForever().withMisfireHandlingInstructionIgnoreMisfires();
            }
            
            logger.info("LogAggregationScheduler starting up");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
            partitionService = hazelcast.getPartitionService();

            localMember = hazelcast.getCluster().getLocalMember();
            hazelcastLdapStore.loadAllKeys();

            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream("/org/dataone/configuration/logQuartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();

            /*
            * The active CN (in the single master CN configuration) is the only CN that will schedule harvest jobs and create new
            * event log entries. Passive CN event indexes are synchronized and recovered currently via Apache SolrCloud */
            if (activeCnIdentifier == null) {
                // The property for the active CN was not set in node.properties, so we will assume that this is 
                // a passive node.
            	logger.warn("cn.nodeId.active property not set in node.properties, so it is assumed that this is a passive CN");
            } else if (localCnIdentifier.compareToIgnoreCase(activeCnIdentifier) == 0) {
				//this.scheduleRecoveryJob();
            	logger.info("Current nodeId ' + localCnIdentifier + ' is the active CN, therefor MN event log harvest jobs will be submitted");
				this.manageHarvest();
            }

            systemMetadataEntryListener.start();

        } catch (IOException ex) {
            throw new IllegalStateException("Loading properties file failedUnable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
        }
    }

    /**
     * will perform the recalculation of the scheduler. 
     * 
     * if scheduler is running, it will be disabled All jobs will be
     * deleted for this node All nodes that are considered 'local' by hazelcast 
     * will be scheduled with log aggregation jobs
     *
     * Seems that the listeners could call this in parallel, and it should
     * be an atomic operation, so it is synchronized
     */
    public synchronized void manageHarvest() throws SchedulerException {
        DateTime startTime = new DateTime();
        // delay the startTime to allow all processing to startup and
        // come to some kind of steady state... might not be possible
        // to predict, but it should be minimally 5-10 minutes
        startTime = startTime.plusMinutes(delayStartOffset);
        // halt all operations
        if (scheduler.isStarted()) {
            scheduler.standby();
            while (!(scheduler.getCurrentlyExecutingJobs().isEmpty())) {
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ex) {
                    logger.warn("Sleep interrupted. check again!");
                }
            }
            // remove any existing jobs
            GroupMatcher<JobKey> groupMatcher = GroupMatcher.groupEquals(logGroupName);
            Set<JobKey> jobsInGroup = scheduler.getJobKeys(groupMatcher);

            for (JobKey jobKey : jobsInGroup) {
                logger.info("deleting job " + jobKey.getGroup() + " " + jobKey.getName());
                scheduler.deleteJob(jobKey);
            }
        }
        // populate the nodeList
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap(hzNodesName);
        JobKey jobKey = new JobKey("job-" + localCnIdentifier, logGroupName);
        // Add the local CN to the jobs to be executed.
        try {
            if (!scheduler.checkExists(jobKey)) {
                JobDataMap jobDataMap = new JobDataMap();
                jobDataMap.put("NodeIdentifier", localCnIdentifier);
                jobDataMap.put("indexLogEntryQueue", indexLogEntryQueue);
                JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity(jobKey).usingJobData(jobDataMap).build();
                TriggerKey triggerKey = new TriggerKey("trigger-" + localCnIdentifier, logGroupName);
                Trigger trigger = newTrigger().withIdentity(triggerKey).startAt(startTime.toDate()).withSchedule(harvestSimpleTriggerSchedule).build();
                logger.info("scheduling job-" + localCnIdentifier + " to start at " + zFmt.print(startTime));
                scheduler.scheduleJob(job, trigger);
            } else {
                logger.error("job-" + localCnIdentifier + " exists!");
            }
        } catch (SchedulerException ex) {
            logger.error("Unable to initialize job key " + localCnIdentifier + " for daily scheduling: ", ex);
        }

        logger.info("Node map has " + hzNodes.size() + " entries");
        // construct new jobs and triggers based on ownership of nodes in the nodeList
        for (NodeReference key : hzNodes.localKeySet()) {
            startTime = startTime.plusSeconds(90);
            Node node = hzNodes.get(key);
            addHarvest(key, node, startTime.toDate());
        }
        scheduler.start();

        if (scheduler.isStarted()) {
            logger.info("Scheduler is started");
        }

    }
    /*
     * Create the specific Trigger and Job that should be executed by Quartz
     * only if the MN is UP and available for synchronization
     * 
     * @param NodeReference 
     * @param Node
     *
     */
    private void addHarvest(NodeReference key, Node node, Date startDate) {
        if (node.getState().equals(NodeState.UP)
                 && node.getType().equals(NodeType.MN)) {

            // the current mn node is owned by this hazelcast cn node member
            // so schedule a job based on the settings of the node

            // Currently, the misfire configuration in the quartz.properties is 5 minutes
            // misfire will cause the trigger to be fired again until successful
            JobKey jobKey = new JobKey("job-" + key.getValue(), logGroupName);
            try {
                if (!scheduler.checkExists(jobKey)) {
                    JobDataMap jobDataMap = new JobDataMap();
                    jobDataMap.put("NodeIdentifier", key.getValue());
                    jobDataMap.put("indexLogEntryQueue", indexLogEntryQueue);
                    JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity(jobKey).usingJobData(jobDataMap).build();
                    TriggerKey triggerKey = new TriggerKey("trigger-" + key.getValue(), logGroupName);
                    Trigger trigger = newTrigger().withIdentity(triggerKey).startAt(startDate).withSchedule(harvestSimpleTriggerSchedule).build();
                    logger.info("scheduling job-" + key.getValue() + " to start at " + zFmt.print(startDate.getTime()));
                    scheduler.scheduleJob(job, trigger);
                } else {
                    logger.error("job-" + key.getValue() + " exists!");
                }
            } catch (SchedulerException ex) {
                logger.error("Unable to initialize job key " + key.getValue() + " for daily scheduling: ", ex);
            }

        }
    }

    /*
     * Determine if log aggregation has ever run before on this machine
     *
     * If log aggregation has never run on any CN, then start up without Recovery If log aggregation has run on this CN,
     * but never on any others then startup without Recovery
     *
     * If log aggregation has never run on this CN, but run on others try to recover all records from another CN
     *
     * If log aggregation has run on this CN and on other CNs, then try to recover all records from where this machines
     * log entries end
     * 
     * Recovery only runs once when the active CN starts.
     *
     */
    public void scheduleRecoveryJob() throws SolrServerException, ServiceFailure, MalformedURLException {
        Boolean recovery = false;
        String recoveryQuery = "";
        NodeAccess nodeAccess = new NodeAccess();
        Map<NodeReference, Map<String, String>> recoveryMap;
        NodeReference localNodeReference = new NodeReference();
        localNodeReference.setValue(localCnIdentifier);
        Date latestRecoverableDate = initializedDate;
        // get all the CNs with their logging status (combination of last date run and aggregation status)
        try {
            recoveryMap = nodeAccess.getCnLoggingStatus();
        } catch (ServiceFailure ex) {
            throw new IllegalStateException("Unable to initialize for recovery: " + ex.getMessage());
        }
        if (recoveryMap != null && !recoveryMap.isEmpty()) {
            // first look at our own state (have we ever run before???)
            Map<String, String> localhostMap = recoveryMap.get(localNodeReference);
            if (localhostMap != null && !localhostMap.isEmpty()) {
                // Get the date of the last log Aggregation
                String localhostLogLastAggregated = localhostMap.get(NodeAccess.LogLastAggregatedAttribute);

                // at least it was initialized, let's see if it is set to a default value of 1900
                // 1900 means it was initialized, but never ran
                if ((localhostLogLastAggregated != null) && initializedDate.before(DateTimeMarshaller.deserializeDateToUTC(localhostLogLastAggregated))) {
                    // This means that the logAggregation has run before. Consider it a Recovery
                    recovery = true;
                    // find out what the last log record is to get the date from it for recovery purposes
                    SolrQuery queryParams = new SolrQuery();
                    queryParams.setQuery("dateAggregated:[* TO NOW]");
                    queryParams.setSortField("dateAggregated", SolrQuery.ORDER.desc);
                    queryParams.setStart(0);
                    queryParams.setRows(1);
                    // this will initialize the https protocol of the solrserver client
                    // to read and send the x509 certificate
                    try {
                        CommonsHttpClientProtocolRegistry.createInstance();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    //
                    // must use https connection because the select filter will require the cn node
                    // subject in order to correctly configure the parameters
                    //
                    String recoveringCnUrl = localhostCNURL.substring(0, localhostCNURL.lastIndexOf("/cn"));
                    recoveringCnUrl += Settings.getConfiguration().getString("LogAggregator.solrUrlPath");
                    CommonsHttpSolrServer recoveringSolrServer = new CommonsHttpSolrServer(recoveringCnUrl);
                    QueryResponse queryResponse = recoveringSolrServer.query(queryParams);
                    List<LogEntrySolrItem> logEntryList = queryResponse.getBeans(LogEntrySolrItem.class);
                    if (!logEntryList.isEmpty()) {
                        // there should only be one
                        LogEntrySolrItem firstSolrItem = logEntryList.get(0);
                        DateTime dt = new DateTime(firstSolrItem.getDateAggregated());

                        DateTime dtUTC = dt.withZone(DateTimeZone.UTC);
                        latestRecoverableDate = dtUTC.toDate();
                        // provide a buffer in case we missed entries because of mis-ordering...
                        dtUTC = dtUTC.minusSeconds(60);
                        recoveryQuery = "dateAggregated:[" + zFmt.print(dtUTC) + " TO NOW]";
                    } else {
                        // What just happened??? the solr index is empty, but this CN has run
                        // log aggregation before. Maybe the index was re-initialized for some reason.
                        logger.warn("localhost solr query should have returned rows but it did not");
                        recoveryQuery = "dateAggregated:[* TO NOW]";
                    }
                }
            }
            if (!recovery) {
                // the logAggregation has never run before, now we need to find out if
                // any other CNs are running logAggregation
                for (NodeReference cnReference : recoveryMap.keySet()) {
                    if (!cnReference.equals(localNodeReference)) {
                        Map<String, String> cnMap = recoveryMap.get(cnReference);
                        String logLastAggregated = cnMap.get(NodeAccess.LogLastAggregatedAttribute);
                        if ((logLastAggregated != null) && initializedDate.before(DateTimeMarshaller.deserializeDateToUTC(logLastAggregated))) {
                            // So it is true, a CN is running logAggregation
                            // this localhost has never run before, but another CN is running
                            // we need to replicate all logs from other CN to this one
                            recoveryQuery = "dateAggregated:[* TO NOW]";
                            recovery = true;
                            break;
                        }
                    }
                }
            }

        } else {
            throw new IllegalStateException("Unable to initialize for recovery: RecoveryMap is empty");
        }
        // schedule the recovery process
        if (recovery) {
            logger.debug("Scheduling Recovery with query " + recoveryQuery);
            // XXX set the status of this CN to recovery
            nodeAccess.setProcessingState(localNodeReference, ProcessingState.Recovery);
            // setup quartz scheduling
            DateTime startTime = new DateTime();
            // provide an offset to ensure the listener is up and running
            // it would be bad to recover before the listener is recording
            // new entries
            startTime = startTime.plusMinutes(delayRecoveryOffset);
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("recoveryQuery", recoveryQuery);
            jobDataMap.put("localhostSolrServer", localhostSolrServer);
            jobDataMap.put("latestRecoverableDate", latestRecoverableDate);
            /*
             * placing Solr Servers in the jobMap is questionable the JobMap entries need to be serialized if ever they
             * become distributed or persisted (not currently) SolrServer class is serializable, but how it is
             * accomplished may influence if this is a viable strategy for future modifications/releases
             *
             * From the quartz documentation: If you use a persistent JobStore (discussed in the JobStore section of
             * this tutorial) you should use some care in deciding what you place in the JobDataMap, because the object
             * in it will be serialized, and they therefore become prone to class-versioning problems. Obviously
             * standard Java types should be very safe, but beyond that, any time someone changes the definition of a
             * class for which you have serialized instances, care has to be taken not to break compatibility.
             *
             * on the other hand, placing them in the JobStore makes unit testing of LogAggregationRecoverJob easier.
             *
             */
            JobKey jobKey = new JobKey("recovery-job" + localCnIdentifier, recoveryGroupName);
            TriggerKey triggerKey = new TriggerKey("recovery-trigger" + localCnIdentifier, recoveryGroupName);
            JobDetail job = newJob(LogAggregationRecoverJob.class).withIdentity(jobKey).usingJobData(jobDataMap).build();
            Trigger trigger = newTrigger().withIdentity(triggerKey).startAt(startTime.toDate()).withSchedule(recoverySimpleTriggerSchedule).build();
            try {
                scheduler.scheduleJob(job, trigger);
            } catch (SchedulerException ex) {
                logger.error("Unable to initialize job key " + localCnIdentifier + " for Job Recovery scheduling: ", ex);
            }
        } else {
            logger.debug("Processing is now Active");
            nodeAccess.setProcessingState(localNodeReference, ProcessingState.Active);
        }
    }
    
    public BlockingQueue<List<LogEntrySolrItem>> getIndexLogEntryQueue() {
        return indexLogEntryQueue;
    }

    public void setIndexLogEntryQueue(BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue) {
        this.indexLogEntryQueue = indexLogEntryQueue;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public void setHazelcast(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public HazelcastLdapStore getHazelcastLdapStore() {
        return hazelcastLdapStore;
    }

    public void setHazelcastLdapStore(HazelcastLdapStore hazelcastLdapStore) {
        this.hazelcastLdapStore = hazelcastLdapStore;
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

    public SolrServer getLocalhostSolrServer() {
        return localhostSolrServer;
    }

    // Injected by Spring
    public void setLocalhostSolrServer(SolrServer localhostSolrServer) {
        this.localhostSolrServer = localhostSolrServer;
    }

    public SystemMetadataEntryListener getSystemMetadataEntryListener() {
        return systemMetadataEntryListener;
    }

    public void setSystemMetadataEntryListener(SystemMetadataEntryListener systemMetadataEntryListener) {
        this.systemMetadataEntryListener = systemMetadataEntryListener;
    }
}
