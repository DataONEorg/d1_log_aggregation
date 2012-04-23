/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
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
