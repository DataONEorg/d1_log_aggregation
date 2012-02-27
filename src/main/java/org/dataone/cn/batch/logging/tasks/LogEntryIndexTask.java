/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.tasks;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.apache.solr.client.solrj.SolrServer;

/**
 * Indexes a list of LogEntrySolrItems
 *
 * Using SolrServer as an argument makes it easier to test
 *
 * It runs as a daemon thread by the LogEntryQueueTask, and
 * is controlled by a thread pool executor
 *
 *
 * @author waltz
 */
public class LogEntryIndexTask implements Callable<String> {

    Logger logger = Logger.getLogger(LogEntryIndexTask.class.getName());

    List<LogEntrySolrItem> indexLogEntryBuffer;
    private SolrServer solrServer;
    public LogEntryIndexTask( SolrServer solrServer, List<LogEntrySolrItem> indexLogEntryBuffer) {
        this.indexLogEntryBuffer = indexLogEntryBuffer;
        this.solrServer = solrServer;
    }
    /**
     *  Implement the Callable interface, providing code to store LogEntry information
     *  in Solr
     *
     *
     * @return String
     * @author waltz
     */
    @Override
    public String call() {

        logger.info("Starting LogEntryIndexTask");
        try {
            solrServer.addBeans(indexLogEntryBuffer);
            solrServer.commit();
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

    /**
     *  Get the SolrServer used in interactions
     *
     *
     * @return SolrServer
     * @author waltz
     */
    public SolrServer getServer() {
        return solrServer;
    }

    /**
     *  Set the SolrServer to be used in interactions
     *
     * @return String
     * @author waltz
     */
    public void setServer(SolrServer solrServer) {
        this.solrServer = solrServer;
    }

}
