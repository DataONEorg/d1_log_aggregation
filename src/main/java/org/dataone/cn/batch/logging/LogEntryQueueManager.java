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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;

/**
 *
 * Manages the BlockingQueue logEntryQueue. 
 * Uses static variable to pass the queue to the LogAggregatorTask that is not configured via
 * Spring configuration.
 *
 *
 * @author waltz
 */
public class LogEntryQueueManager {

    private static BlockingQueue<List<LogEntrySolrItem>> logEntryQueue ;
    static LogEntryQueueManager logEntryQueueManager = null;
    private LogEntryQueueManager() {
        logEntryQueue = new ArrayBlockingQueue<List<LogEntrySolrItem>>(50000, true);
    }
    public static BlockingQueue<List<LogEntrySolrItem>> getLogEntryQueue() {
        return logEntryQueue;
    }

    public static void setLogEntryQueue(BlockingQueue<List<LogEntrySolrItem>> indexLogEntryQueue) {
        logEntryQueue = indexLogEntryQueue;
    }
    public static LogEntryQueueManager getInstance() {
        if (logEntryQueueManager == null) {
            logEntryQueueManager = new LogEntryQueueManager();
        }
        return logEntryQueueManager;
    }
}
