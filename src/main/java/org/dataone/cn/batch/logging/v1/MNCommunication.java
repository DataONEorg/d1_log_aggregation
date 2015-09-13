package org.dataone.cn.batch.logging.v1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;


import org.apache.log4j.Logger;
import org.dataone.client.v1.itk.D1Client;
import org.dataone.client.v1.MNode;
import org.dataone.cn.batch.logging.exceptions.QueryLimitException;
import org.dataone.cn.batch.logging.tasks.LogAggregatorTask;

import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.batch.logging.type.LogQueryDateRange;
import org.dataone.cn.batch.logging.NodeHarvester;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.LogEntry;


public class MNCommunication implements NodeHarvester {
    public Integer batchSize = Settings.getConfiguration().getInt("LogAggregator.logRecords_batch_size", 1000);
    private static final long serialVersionUID = 10000001;
	 /**
     * performs the retrieval of the log records  from a DataONE node.
     * It retrieves the list in batches and should be called iteratively
     * until all log entries have been retrieved from a node.
	 * @param logQueryStack 
     * 
     * @return List<LogEntry>
     */
    protected NodeReference d1NodeReference;
    public MNCommunication (NodeReference d1NodeReference) {
        this.d1NodeReference = d1NodeReference;
    }
    public List<LogEntrySolrItem> harvest(Stack<LogQueryDateRange> logQueryStack, Integer queryTotalLimit) throws ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented, InvalidToken, QueryLimitException 
    		 {
        // logger is not  be serializable, but no need to make it transient imo
        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        List<LogEntrySolrItem> writeQueue = new ArrayList<LogEntrySolrItem>();
        try {
            LogQueryDateRange logQueryDateRange = logQueryStack.pop();
            
            int start = 0;
            int total = 0;
            Log logList = null;

            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " starting retrieval from " + start);
            do {
               boolean activateJob = Boolean.parseBoolean(Settings.getConfiguration().getString("LogAggregator.active"));
                if (!activateJob) {
                    logQueryStack.empty();
                    logger.warn("LogAggregatorTask-" + d1NodeReference.getValue() + "QueryStack is Emptied because LogAggregation has been de-activated");
                    throw new EmptyStackException();
                }
                MNode mNode = D1Client.getMN(d1NodeReference);
				// always execute for the first run (for start = 0)
                // otherwise skip because when the start is equal or greater
                // then total, then all objects have been harvested
                // based on information from metacat devs, first querying with
                // rows 0 will return quickly due to the
                // shortcut of not needing to perform paging
                if (start == 0) {
                    try {
                        logList = mNode.getLogRecords(null, logQueryDateRange.getFromDate(), logQueryDateRange.getToDate(), null, null, 0, 0);
                    } catch (NotAuthorized e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (InvalidRequest e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (NotImplemented e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (ServiceFailure e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    } catch (InvalidToken e) {
                        logQueryStack.push(logQueryDateRange);
                        throw e;
                    }
                    // if objectList is null or the count is 0 or the list is empty, then
                    // there is nothing to process
                    if (logList != null) {
                        // if the total records returned from the above query is greater than the
                        // limit we have placed on batch processing, then find the median date
                        // and try again.
                        // or if the date range of the query is less that one second, return the results as found
                        // even if they extend beyon the batch processing limit
                        if ((logList.getTotal() > queryTotalLimit)
                                && (logQueryDateRange.getToDate().getTime() - logQueryDateRange.getFromDate().getTime()) > 1000L) {
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " discard pop start " + format.format(logQueryDateRange.getFromDate()) + " end " + format.format(logQueryDateRange.getToDate()));
                            long medianTime = (logQueryDateRange.getFromDate().getTime() + logQueryDateRange.getToDate().getTime()) / 2;

                            LogQueryDateRange lateRecordDate = new LogQueryDateRange(new Date(medianTime), logQueryDateRange.getToDate());
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() +" late push start " + format.format(lateRecordDate.getFromDate()) + " end " + format.format(lateRecordDate.getToDate()));
                            logQueryStack.push(lateRecordDate);

                            LogQueryDateRange earlyRecordDate = new LogQueryDateRange(logQueryDateRange.getFromDate(), new Date(medianTime));
                            logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + "early push start " + format.format(earlyRecordDate.getFromDate()) + " end " + format.format(earlyRecordDate.getToDate()));
                            logQueryStack.push(earlyRecordDate);
                            throw new QueryLimitException();
                        }
                    }
                }
                
                try {
                    logList = mNode.getLogRecords(null, logQueryDateRange.getFromDate(), logQueryDateRange.getToDate(), null, null, start, batchSize);
                } catch (NotAuthorized e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (InvalidRequest e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (NotImplemented e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (ServiceFailure e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                } catch (InvalidToken e) {
                    logQueryStack.push(logQueryDateRange);
                    throw e;
                }
                // if objectList is null or the count is 0 or the list is empty, then
                // there is nothing to process


                if ((logList != null)
                        && (logList.getLogEntryList() != null)
                        && (logList.getCount() > 0)
                        && (!logList.getLogEntryList().isEmpty())) {
                    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " harvested start " + format.format(logQueryDateRange.getFromDate()) + " end " + format.format(logQueryDateRange.getToDate()));
                    logger.debug("LogAggregatorTask-" + d1NodeReference.getValue() + " log harvested start#=" + logList.getStart() + " count=" + logList.getCount() + " total=" + logList.getTotal());
                    start += logList.getCount();
                    total = logList.getTotal();
                    
                    for (LogEntry logEntry : logList.getLogEntryList()) {
                        LogEntrySolrItem solrItem = new LogEntrySolrItem(logEntry);
                        writeQueue.add(solrItem);
                    }
                    
                }
                if ((logList.getLogEntryList() == null)) {
                    logger.error("How did we get a returned object that has a null LogEntry List? Creating a new Log creates an empty array list");
                }
            } while ((logList != null) && (logList.getCount() > 0) && (start < total));
        } catch (EmptyStackException ex) {
            throw ex;
        }
        return writeQueue;
    }

    @Override
    public NodeReference getNodeReference() {
        return d1NodeReference;
    }

    @Override
    public void setNodeReference(NodeReference nodeReference) {
        d1NodeReference = nodeReference;
        
    }

}
