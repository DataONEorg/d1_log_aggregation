/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

import org.dataone.cn.batch.logging.listener.SystemMetadataEntryListener;
import org.dataone.cn.batch.logging.listener.LogEntryTopicListener;
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
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.core.Hazelcast;
import java.util.List;
import java.util.ArrayList;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.core.Member;
import org.dataone.service.types.v1.NodeReference;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import java.util.logging.Level;
import java.util.logging.Logger;
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
import java.util.Set;
import org.dataone.cn.hazelcast.HazelcastLdapStore;
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

import static org.quartz.JobBuilder.*;

/**
 * this bean must be managed by Spring
 * upon startup of spring it will execute via init method
 * 
 * evaluate whether the NodeList contains nodes that should be harvested for logs on the
 * executing coordinating node. It will add  or remove triggers for jobs based on
 * events, such as startup, nightly refactoring, more CNs coming online
 *
 * 
 * @author waltz
 */
public class LogAggregationScheduleManager implements ApplicationContextAware, EntryListener<NodeReference, Node>, MigrationListener {

    private String clientCertificateLocation =
            Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");
    public static Log logger = LogFactory.getLog(LogAggregationScheduleManager.class);
    private static String groupName = "LogAggregatorHarvesting";
    private HazelcastInstance hazelcast;
    private HazelcastLdapStore hazelcastLdapStore;
    private Scheduler scheduler;
    ApplicationContext applicationContext;
    PartitionService partitionService;
    Member localMember;
    private SolrServer localhostSolrServer;
    private LogEntryTopicListener logEntryTopicListener;
    private SystemMetadataEntryListener systemMetadataEntryListener;
    private static SimpleScheduleBuilder simpleTriggerSchedule = null;

    private static SimpleScheduleBuilder recoveryTriggerSchedule = simpleSchedule().withRepeatCount(0).withMisfireHandlingInstructionFireNow();
    static final DateTimeFormatter zFmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final Date initializedDate = DateTimeMarshaller.deserializeDateToUTC("1900-01-01T00:00:00.000-00:00");
    static final String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");
    static final int delayStartOffset = Settings.getConfiguration().getInt("LogAggregator.delayStartOffset.minutes");
    static final int delayRecoveryOffset = Settings.getConfiguration().getInt("LogAggregator.delayRecoveryOffset.minutes");
    public void init() {
        try {
            int triggerIntervalPeriod = Settings.getConfiguration().getInt("LogAggregator.triggerInterval.period");
            String triggerIntervalPeriodField = Settings.getConfiguration().getString("LogAggregator.triggerInterval.periodField");
            if (triggerIntervalPeriodField.equalsIgnoreCase("seconds")) {
                simpleTriggerSchedule = simpleSchedule().withIntervalInSeconds(triggerIntervalPeriod).repeatForever().withMisfireHandlingInstructionFireNow();
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("minutes")) {
                simpleTriggerSchedule = simpleSchedule().withIntervalInMinutes(triggerIntervalPeriod).repeatForever().withMisfireHandlingInstructionFireNow();
            } else if (triggerIntervalPeriodField.equalsIgnoreCase("hours")) {
                 simpleTriggerSchedule = simpleSchedule().withIntervalInHours(triggerIntervalPeriod).repeatForever().withMisfireHandlingInstructionFireNow();
            } else {
                simpleSchedule().withIntervalInHours(24).repeatForever().withMisfireHandlingInstructionFireNow();
            }
            logger.info("LogAggregationScheduler starting up");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
            partitionService = Hazelcast.getPartitionService();

            localMember = hazelcast.getCluster().getLocalMember();
            hazelcastLdapStore.loadAllKeys();

            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream("/org/dataone/configuration/logQuartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();

            this.scheduleRecoveryJob();

            // the LogEntryTopicListener should only be started after JobRecovery
            // since we don't want any indexing jobs spawned by interactions with the
            // LogEntry topic to corrupt the index
            // before the most recent log entry is retrieved from the index

            logEntryTopicListener.addListener();
            systemMetadataEntryListener.start();
            this.manageHarvest();
            partitionService.addMigrationListener(this);
            IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");
            hzNodes.addEntryListener(this, true);
        } catch (SolrServerException ex) {
            throw new IllegalStateException("SolrServer connection failed: " + ex.getMessage());
        } catch (IOException ex) {
            throw new IllegalStateException("Loading properties file failedUnable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (ServiceFailure ex) {
            throw new IllegalStateException("NodeService failed: " + ex.getMessage());
        }
    }

    public void manageHarvest() throws SchedulerException {
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
            GroupMatcher<JobKey> groupMatcher = GroupMatcher.groupEquals(groupName);
            Set<JobKey> jobsInGroup = scheduler.getJobKeys(groupMatcher);

            for (JobKey jobKey : jobsInGroup) {
                logger.info("deleting job " + jobKey.getGroup() + " " + jobKey.getName());
                scheduler.deleteJob(jobKey);
            }
        }
        // populate the nodeList
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

        // Add the local CN to the jobs to be executed.
        JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity("job-" + localCnIdentifier, groupName).usingJobData("NodeIdentifier", localCnIdentifier).build();
        Trigger trigger = newTrigger().withIdentity("trigger-" + localCnIdentifier, groupName).startAt(startTime.toDate()).withSchedule(simpleTriggerSchedule).build();
        try {
            JobKey jobKey = new JobKey("job-" + localCnIdentifier, groupName);
            if (!scheduler.checkExists(jobKey)) {
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

    private void addHarvest(NodeReference key, Node node, Date startDate) {
        if (node.getState().equals(NodeState.UP)
                && node.isSynchronize() && node.getType().equals(NodeType.MN)) {

            // the current mn node is owned by this hazelcast cn node member
            // so schedule a job based on the settings of the node

            // Currently, the misfire configuration in the quartz.properties is 5 minutes
            // misfire will cause the trigger to be fired again until successful
            JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity("job-" + key.getValue(), groupName).usingJobData("NodeIdentifier", key.getValue()).build();
            Trigger trigger = newTrigger().withIdentity("trigger-" + key.getValue(), groupName).startAt(startDate).withSchedule(simpleTriggerSchedule).build();
            try {
                JobKey jobKey = new JobKey("job-" + key.getValue(), groupName);
                if (!scheduler.checkExists(jobKey)) {
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
     * monitor node additions
     * 
     * additions to hazelcast should only be noted if an administrator approves a node(?)
     *
     */

    @Override
    public void entryAdded(EntryEvent<NodeReference, Node> event) {
        logger.info("Node Entry added key=" + event.getKey().getValue());
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException ex) {
            Logger.getLogger(LogAggregationScheduleManager.class.getName()).log(Level.SEVERE, null, ex);
        }

        Partition partition = partitionService.getPartition(event.getKey());
        Member ownerMember = partition.getOwner();

        if (localMember.equals(ownerMember)) {
            try {
                this.manageHarvest();
            } catch (SchedulerException ex) {
                throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
            }
        }
    }

    @Override
    public void entryRemoved(EntryEvent<NodeReference, Node> event) {
        logger.error("Entry removed key=" + event.getKey().getValue());
    }
    /*
     * monitor node changes
     * updates to hazelcast should only be noted if updateNodeCapabilities is called on the CN
     *
     */

    @Override
    public void entryUpdated(EntryEvent<NodeReference, Node> event) {
        logger.info("Node Entry updated key=" + event.getKey().getValue());

        try {
            Thread.sleep(2000L);
        } catch (InterruptedException ex) {
            Logger.getLogger(LogAggregationScheduleManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        Partition partition = partitionService.getPartition(event.getKey());
        Member ownerMember = partition.getOwner();

        if (localMember.equals(ownerMember)) {
            if (localMember.equals(ownerMember)) {
                try {
                    this.manageHarvest();
                } catch (SchedulerException ex) {
                    throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
                }
            }
        }
    }

    @Override
    public void entryEvicted(EntryEvent<NodeReference, Node> event) {
        logger.warn("Entry evicted key=" + event.getKey().getValue());
    }

    //
    // http://code.google.com/p/hazelcast/source/browse/trunk/hazelcast/src/main/java/com/hazelcast/impl/TestUtil.java?r=1824
    // http://groups.google.com/group/hazelcast/browse_thread/thread/3856d5829e26f81c?fwc=1
    //
    /*
     * if a migration has occurred between CNs, then we need to figure
     * out if nodes have changed their home machine
     * only the nodes 'owned' by a local machine should be
     * scheduled by that machine
     * 
     */
    public void migrationCompleted(MigrationEvent migrationEvent) {
        logger.debug("migrationCompleted " + migrationEvent.getPartitionId());
        // this is the partition that was moved from 
        // one node to the other
        // try to determine if a Node has migrated home servers
        if (localMember.equals(migrationEvent.getNewOwner()) || localMember.equals(migrationEvent.getOldOwner())) {
            Integer partitionId = migrationEvent.getPartitionId();
            PartitionService partitionService = Hazelcast.getPartitionService();

            IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

            List<Integer> nodePartitions = new ArrayList<Integer>();
            for (NodeReference key : hzNodes.keySet()) {
                nodePartitions.add(partitionService.getPartition(key).getPartitionId());
            }

            if (nodePartitions.contains(partitionId)) {
                logger.info("Node Partions migrated ");
                try {
                    this.manageHarvest();
                } catch (SchedulerException ex) {
                    throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
                }
            }
        }
    }

    public void migrationStarted(MigrationEvent migrationEvent) {
        logger.debug("migrationStarted " + migrationEvent.getPartitionId());
    }

    /*
     * Determine if log aggregation has ever run before on this machine
     *
     * If log aggregation has never run on any CN, then start up without
     * Recovery
     * If log aggregation has run on this CN, but never on any others
     * then startup without Recovery
     *
     * If log aggregation has never run on this CN, but run on others
     * try to recover all records from another CN
     *
     * If log aggreation has run on this CN and on other CNs,
     * then try to recover all records from where this machines log entries
     * end
     * 
     */
    public void scheduleRecoveryJob() throws SolrServerException, ServiceFailure {
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

                    QueryResponse queryResponse = localhostSolrServer.query(queryParams);
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
            // setup quarz scheduling
            DateTime startTime = new DateTime();
            // provide an offset to ensure the listener is up and running
            // it would be bad to recover before the listener is recording
            // new entries
            startTime = startTime.plusMinutes(delayRecoveryOffset);
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("recoveryQuery", recoveryQuery);
            jobDataMap.put("localhostSolrServer", localhostSolrServer);
            jobDataMap.put("latestRecoverableDate", latestRecoverableDate);
            /* placing Solr Servers in the jobMap is questionable
             * the JobMap entries need to be serialized if
             * ever they become distributed or persisted (not currently)
             * SolrServer class is serializable, but how it is accomplished
             * may influence if this is a viable strategy for future
             * modifications/releases
             *
             * From the quartz documentation:
             * If you use a persistent JobStore (discussed in the JobStore section of this tutorial)
             * you should use some care in deciding what you place in the JobDataMap,
             * because the object in it will be serialized, and they therefore become prone to
             * class-versioning problems. Obviously standard Java types should be very safe,
             * but beyond that, any time someone changes the definition of a class for which
             * you have serialized instances, care has to be taken not to break compatibility.
             *
             * on the other hand, placeing them in the JobStore makes unit testing of
             * LogAggregationRecoverJob easier.
             * 
             */

            JobDetail job = newJob(LogAggregationRecoverJob.class).withIdentity("job-recover" + localCnIdentifier, groupName).usingJobData(jobDataMap).build();
            Trigger trigger = newTrigger().withIdentity("trigger-recover" + localCnIdentifier, groupName).startAt(startTime.toDate()).withSchedule(recoveryTriggerSchedule).build();
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

    public void setLocalhostSolrServer(SolrServer localhostSolrServer) {
        this.localhostSolrServer = localhostSolrServer;
    }

    public LogEntryTopicListener getLogEntryTopicListener() {
        return logEntryTopicListener;
    }

    public void setLogEntryTopicListener(LogEntryTopicListener logEntryTopicListener) {
        this.logEntryTopicListener = logEntryTopicListener;
    }

    public SystemMetadataEntryListener getSystemMetadataEntryListener() {
        return systemMetadataEntryListener;
    }

    public void setSystemMetadataEntryListener(SystemMetadataEntryListener systemMetadataEntryListener) {
        this.systemMetadataEntryListener = systemMetadataEntryListener;
    }

}
