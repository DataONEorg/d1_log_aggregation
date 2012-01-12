/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dataone.service.types.v1.LogEntry;
import java.text.SimpleDateFormat;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.apache.solr.client.solrj.SolrServer;

/**
 * Reads from the LogEvent tasks that need to be indexed.
 *
 * Keeps track of the number of tasks that have been published
 * by use on an internal queue. When the queue is 'full' or after an
 * established wait period, deliver the contents of the queue to the index
 *
 * It runs as a daemon thread by the LogEntryIndexExecutor, and
 * is run as an eternal loop unless an exception is thrown.
 *
 *
 * @author waltz
 */
public class LogEntryIndexTask implements Callable<String> {

    Logger logger = Logger.getLogger(LogEntryIndexTask.class.getName());

    SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss zzz");
    List<LogEntrySolrItem> indexLogEntryBuffer;
    private SolrServer solrServer;
    public LogEntryIndexTask( SolrServer solrServer, List<LogEntrySolrItem> indexLogEntryBuffer) {
        this.indexLogEntryBuffer = indexLogEntryBuffer;
        this.solrServer = solrServer;
    }
    @Override
    public String call() {

        logger.info("Starting LogEntryIndexTask");
        try {
            solrServer.addBeans(indexLogEntryBuffer);
        } catch (SolrServerException ex) {
            // what to do with this exception??? looks bad.
            ex.printStackTrace();
        } catch (IOException ex) {
             // what to do with this exception??? retry?
            ex.printStackTrace();
        }
         logger.info("Ending LogEntryIndexTask");
        return "Done";
    }


    public SolrServer getServer() {
        return solrServer;
    }

    public void setServer(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

}
