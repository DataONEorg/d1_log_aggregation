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

import org.dataone.cn.batch.logging.tasks.*;
import org.apache.log4j.Logger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 *
 * Manages the LogEntryQueueTask by submitting it to an Executor
 * If the LogEntryQueueTask should return from execution,
 * evaluate the conditions by which it returned and determine
 * If the task may be executed again.
 *
 * TODO: If the log entry queue executor dies, the entire logAggregation should
 * be shutdown and a report sent to someone about its failure!
 * 
 * @author waltz
 */
public class LogEntryQueueManager  implements Runnable {

    Logger logger = Logger.getLogger(LogEntryQueueManager.class.getName());

    private SimpleAsyncTaskExecutor taskExecutor;
    LogEntryQueueTask logEntryQueueTask;
    // logEntryQueueManagerFuture future probabaly is not needed,
    // but maybe it will force the executor to remove the thread (???)
    Future logEntryQueueManagerFuture = null;
    public void init() {
        logEntryQueueManagerFuture =  taskExecutor.submit(this);
    }
    public void run() {
        boolean shouldContinueRunning = true;
        do {
            logger.debug("Starting LogEntryQueueManager");
             FutureTask futureTask = new FutureTask(logEntryQueueTask);
             taskExecutor.execute(futureTask);
            try {
                futureTask.get();
            } catch (InterruptedException ex) {
                logger.warn( ex.getMessage());
            } catch (ExecutionException ex) {
                ex.printStackTrace();
                shouldContinueRunning = false;
            } catch (Exception ex) {
                ex.printStackTrace();
                shouldContinueRunning = false;
            }
            if (futureTask.isCancelled()) {
                logger.warn("logEntryQueueTask was cancelled");
                shouldContinueRunning = false;
            } else {
                 futureTask.cancel(true);
            }
        } while (shouldContinueRunning);
        logEntryQueueManagerFuture.cancel(true);
    }

    public LogEntryQueueTask getLogEntryQueueTask() {
        return logEntryQueueTask;
    }

    public void setLogEntryQueueTask(LogEntryQueueTask logEntryQueueTask) {
        this.logEntryQueueTask = logEntryQueueTask;
    }

    public SimpleAsyncTaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(SimpleAsyncTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

}
