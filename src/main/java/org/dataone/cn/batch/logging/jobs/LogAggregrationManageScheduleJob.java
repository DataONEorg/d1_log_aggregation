/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging.jobs;


import org.apache.log4j.Logger;
import org.dataone.cn.batch.logging.LogAggregationScheduleManager;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *
 * @author waltz
 */
@DisallowConcurrentExecution
public class LogAggregrationManageScheduleJob implements Job {

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        Logger logger = Logger.getLogger(LogAggregationHarvestJob.class.getName());

        JobExecutionException jex = null;
        try {
            LogAggregationScheduleManager.getInstance().scheduleHarvest();
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Job- " + jec.getJobDetail().getKey().getGroup() + ":" + jec.getJobDetail().getKey().getName() + " died: " + ex.getMessage());
            jex = new JobExecutionException();
            jex.setStackTrace(ex.getStackTrace());
        }
        if (jex != null) {
            throw jex;
        }
        
    }
    
}
