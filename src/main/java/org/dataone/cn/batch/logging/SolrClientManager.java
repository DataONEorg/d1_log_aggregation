/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging;

import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.configuration.Settings;

/**
 *
 * @author waltz
 */
public class SolrClientManager {
    Logger logger = Logger.getLogger(SolrClientManager.class.getName());
    static SolrClientManager solrClientManager = null;
    private HttpSolrClient httpSolrClient = null;
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
    
    public synchronized Boolean submitBeans(List<LogEntrySolrItem> indexLogEntryBuffer) throws SolrServerException, IOException{
        Boolean submitted = true;
        logger.info("LogEntryIndexTask adding " + indexLogEntryBuffer.size() + " records to index");

            
            httpSolrClient.addBeans(indexLogEntryBuffer);
            httpSolrClient.commit();

        logger.info("Ending LogEntryIndexTask");
        return submitted;
    }
    
}
