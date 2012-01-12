/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging;

import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Use an executor that will only manage a single thread per execution of a task
 * This is an Executor that will manage each CN's localhost execution of
 * getLogRecords.
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
        }
        return simpleTaskExecutor;
    }


   
}
