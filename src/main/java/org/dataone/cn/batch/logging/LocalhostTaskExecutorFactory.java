/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging;

import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Use an executor that will only manage a single thread per execution of a task
 * This is an Executor that will manage each CN's localhost execution of
 * getLogRecords. Note that this Executor will not allow concurrent threads
 *
 * This Factory is only used by LogAggregationHarvestJob
 * 
 * @author waltz
 */
public class LocalhostTaskExecutorFactory {
    static SimpleAsyncTaskExecutor simpleTaskExecutor = null;


    public static SimpleAsyncTaskExecutor getSimpleTaskExecutor() {
        if (LocalhostTaskExecutorFactory.simpleTaskExecutor == null) {
            simpleTaskExecutor = new SimpleAsyncTaskExecutor();
            simpleTaskExecutor.setConcurrencyLimit(SimpleAsyncTaskExecutor.NO_CONCURRENCY);
            simpleTaskExecutor.setDaemon(true);
            simpleTaskExecutor.setThreadGroupName("SimpleAsyncLogGroup");
            simpleTaskExecutor.setThreadNamePrefix("LocalhostIndexLogTask");
        }
        return simpleTaskExecutor;
    }


   
}
