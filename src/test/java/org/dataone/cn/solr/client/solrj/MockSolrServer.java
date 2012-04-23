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

package org.dataone.cn.solr.client.solrj;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.solr.client.solrj.SolrRequest;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Mock implementation of SolrServer for testing. Cut and Paste of SolrServer.java
 * with minor alterations, pretty much need to edit if ever returning information
 * becomes significant for determining success of unit test
 *
 * @author waltz
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 * @version 3.4.0 : SolrServer.java 824332 2009-10-12 13:40:03Z ehatcher $
 * @since solr 1.3
 *
 */
public class MockSolrServer extends SolrServer implements Serializable {

    private DocumentObjectBinder binder;
    Collection<?> addedBeans = null;

    @Override
    public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }

    @Override
    public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
        addedBeans = beans;
        UpdateResponse response = new UpdateResponse();
        return response;
    }

    @Override
    public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }

    @Override
    public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
        return add(getBinder().toSolrInputDocument(obj));
    }

    /** waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
     * @throws IOException
     */
    @Override
    public UpdateResponse commit() throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }

    /** waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
     * @throws IOException
     */
    @Override
    public UpdateResponse optimize() throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse rollback() throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
        UpdateResponse response = new UpdateResponse();
        return response;
    }
    @Override
    public SolrPingResponse ping() throws SolrServerException, IOException {
        SolrPingResponse pingResponse = new SolrPingResponse();
        return pingResponse;
    }
    @Override
    public QueryResponse query(SolrParams params) throws SolrServerException {
        QueryResponse queryResponse = new QueryResponse();
        return queryResponse;
    }
    @Override
    public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException {
        QueryResponse queryResponse = new QueryResponse();
        return queryResponse;
    }

    /**
     * SolrServer implementations need to implement how a request is actually processed
     * @param request
     * @return
     * @throws SolrServerException
     * @throws IOException
     */
    @Override
    public NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException {
        NamedList<Object> namedList = new NamedList<Object>();
        return namedList;
    }
    @Override
    public DocumentObjectBinder getBinder() {
        if (binder == null) {
            binder = new DocumentObjectBinder();
        }
        return binder;
    }

    public Collection<?> getAddedBeans() {
        return addedBeans;
    }
    
}
