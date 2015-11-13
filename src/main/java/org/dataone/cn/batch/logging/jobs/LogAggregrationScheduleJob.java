/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class LogAggregrationScheduleJob implements Job {

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        Log logger = LogFactory.getLog(LogAggregationHarvestJob.class);
        JobExecutionException jex = null;
        try {
            LogAggregationScheduleManager.getInstance().manageHarvest();
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
