/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.NodeReference;

/**
 *
 * @author waltz
 */
public class SolrClientManager {
    Logger logger = Logger.getLogger(SolrClientManager.class.getName());
    static SolrClientManager solrClientManager = null;
    private HttpSolrClient httpSolrClient = null;
    private final Lock solrClientLock = new ReentrantLock();
    private SolrClientManager() {
        String localhostSolrURL = Settings.getConfiguration().getString("LogAggregator.solrUrl");
        httpSolrClient = new HttpSolrClient(localhostSolrURL);
    }
    
    public static SolrClientManager getInstance() {
        if (solrClientManager == null) {
            solrClientManager = new SolrClientManager();
        }
        return solrClientManager;
    }
    
    public Boolean submitBeans(NodeReference d1NodeReference, List<LogEntrySolrItem> indexLogEntryBuffer) throws SolrServerException, IOException{
        logger.info("SolrClientManager-" + d1NodeReference.getValue() + " submitting " + indexLogEntryBuffer.size() + " records to index");
        solrClientLock.lock();
        Boolean submitted = true;
        logger.debug("SolrClientManager-" + d1NodeReference.getValue() + " locked ");
        try {
            
            httpSolrClient.addBeans(indexLogEntryBuffer);
            httpSolrClient.commit();
            logger.info("SolrClientManager-" + d1NodeReference.getValue() + " Completed submission");
        } finally {
            solrClientLock.unlock();
            logger.debug("SolrClientManager-" + d1NodeReference.getValue() + " unlocked ");
        }

        
        return submitted;
    }
    
}
