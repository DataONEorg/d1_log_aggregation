/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.solr.client.solrj.impl;

import java.net.MalformedURLException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

/**
 * Solr From 1.3.0 - 3.4.0, 3.5.0 this is deprecated and we can remove this
 * wrapper class and use the recommended one.
 * But since i need to configure this via spring, and spring doesn't like
 * overloaded (and deprecated) setter methods. create a separately named method
 * that will call the live method until we upgrade our solr...
 * 
 * @author waltz
 */
public class LocalhostCommonsHttpSolrServer extends CommonsHttpSolrServer {

   public LocalhostCommonsHttpSolrServer(String solrServerUrl) throws MalformedURLException {
       super(solrServerUrl);
   }
   public void setConnectManagerTimeout(long timeout) {
       super.setConnectionManagerTimeout(timeout);
   }
}
