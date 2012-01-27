/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

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
import org.dataone.cn.hazelcast.ldap.HazelcastLdapStore;
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
 * evaluate whether the NodeList contains nodes that should be executed on the
 * executing coordinating node. it will add  or remove triggers for jobs based on
 * events, such as startup, nightly refactoring, more CNs coming online
 *
 * todo: add in nightly job that re-calcuates jobs
 *       add in listeners that will,under certain conditions, add a job to call manager
 *       added jobs that call the manager should retrieve the manager from spring context
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
    private static SimpleScheduleBuilder simpleTriggerSchedule = simpleSchedule().withIntervalInHours(24).repeatForever().withMisfireHandlingInstructionFireNow();
//    private static SimpleScheduleBuilder simpleTriggerSchedule = simpleSchedule().withIntervalInMinutes(2).repeatForever().withMisfireHandlingInstructionFireNow();
    static final String localCnIdentifier = Settings.getConfiguration().getString("cn.nodeId");
    public void init() {
        try {
            logger.info("LogAggregationScheduler starting up");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
            partitionService = Hazelcast.getPartitionService();

            localMember = hazelcast.getCluster().getLocalMember();
            hazelcastLdapStore.loadAllKeys();
            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream("/org/dataone/configuration/logQuartz.properties"));
            StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();


            this.manageHarvest();
            partitionService.addMigrationListener(this);
            IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");
            hzNodes.addEntryListener(this, true);
        } catch (IOException ex) {
            throw new IllegalStateException("Loading properties file failedUnable to initialize jobs for scheduling: " + ex.getMessage());
        } catch (SchedulerException ex) {
            throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
        }
    }

    public void manageHarvest() throws SchedulerException {
        DateTime startTime = new DateTime();
         startTime = startTime.plusSeconds(90);
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
                scheduler.deleteJob(jobKey);
            }
        }
        // populate the nodeList
        IMap<NodeReference, Node> hzNodes = hazelcast.getMap("hzNodes");

        // Add the local CN to the jobs to be executed.
        JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity("job-" + localCnIdentifier, groupName).usingJobData("NodeIdentifier", localCnIdentifier).build();
        Trigger trigger = newTrigger().withIdentity("trigger-" + localCnIdentifier, groupName).startAt(startTime.toDate()).withSchedule(simpleTriggerSchedule).build();
        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException ex) {
            logger.error("Unable to initialize job key " + localCnIdentifier + " for daily scheduling: ", ex);
        }

        logger.info("Node map has " + hzNodes.size() + " entries");
        // construct new jobs and triggers based on ownership of nodes in the nodeList
        
        for (NodeReference key : hzNodes.localKeySet()) {
            //  membernodes that are down or do not
            // want to be synchronized
            startTime = startTime.plusSeconds(90);
            Node node = hzNodes.get(key);
            addHarvest(key, node, startTime.toDate());
        }
        scheduler.start();

        if (scheduler.isStarted()) {
            logger.debug("Scheduler is started");
        }

    }


    private void addHarvest (NodeReference key, Node node, Date startDate) {
            if (node.getState().equals(NodeState.UP)
                    && node.isSynchronize() && node.getType().equals(NodeType.MN)) {
                
                // the current mn node is owned by this hazelcast cn node member
                // so schedule a job based on the settings of the node

                // Currently, the misfire configuration in the quartz.properties is 5 minutes
                // misfire will cause the trigger to be fired again until successful
                JobDetail job = newJob(LogAggregationHarvestJob.class).withIdentity("job-" + key.getValue(), groupName).usingJobData("NodeIdentifier", key.getValue()).build();
                Trigger trigger = newTrigger().withIdentity("trigger-" + key.getValue(), groupName).startAt(startDate).withSchedule(simpleTriggerSchedule).build();
                try {
                    scheduler.scheduleJob(job, trigger);
                } catch (SchedulerException ex) {
                    logger.error("Unable to initialize job key " + key.getValue() + " for daily scheduling: ", ex);
                }

            }
    }
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
            logger.debug("Should not be here");
            // try {
                // need a mechanism to turn off the job, update the crontab entry if node is still active
                // and schedule it again
            // } catch (SchedulerException ex) {
            //    throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
            // }

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
    public void migrationCompleted(MigrationEvent migrationEvent) {
        logger.warn("migrationCompleted " + migrationEvent.getPartitionId());
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
                try {
                    this.manageHarvest();
                } catch (SchedulerException ex) {
                    throw new IllegalStateException("Unable to initialize jobs for scheduling: " + ex.getMessage());
                }
            }
        }


    }

    public void migrationStarted(MigrationEvent migrationEvent) {
        logger.warn("migrationStarted " + migrationEvent.getPartitionId());
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
}
