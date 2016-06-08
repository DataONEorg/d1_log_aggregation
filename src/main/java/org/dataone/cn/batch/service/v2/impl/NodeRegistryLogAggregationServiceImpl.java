/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.service.v2.impl;

import java.util.Date;
import org.dataone.cn.batch.service.v2.NodeRegistryLogAggregationService;
import org.dataone.cn.ldap.NodeRegistryLogAggregationFacade;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

/**
 *  Currently the service is handled by a Facade around the NodeRegistry
 *  However, when log aggregation is moved to a separate machine
 *  other than the CN, the use of ldap will be abandoned.
 *  listNodes and getNode may be performed by libclient java calls.
 *  getAggregateLogs, getLogLastAggregated, setAggregateLogs and
 *  setLogLastAggregated may be persisted to a separate data store
 *  
 * @author waltz
 */
public class NodeRegistryLogAggregationServiceImpl implements NodeRegistryLogAggregationService {

                private NodeRegistryLogAggregationFacade nodeRegistryLogAggregationFacade
                    = new NodeRegistryLogAggregationFacade();
    @Override
    public NodeList listNodes() throws ServiceFailure, NotImplemented {
        return nodeRegistryLogAggregationFacade.getApprovedNodeList();
    }

    @Override
    public Node getNode(NodeReference nodeId) throws NotFound, ServiceFailure {
        return nodeRegistryLogAggregationFacade.getNode(nodeId);
    }

    @Override
    public Boolean getAggregateLogs(NodeReference nodeIdentifier) throws ServiceFailure {
        return nodeRegistryLogAggregationFacade.getAggregateLogs(nodeIdentifier);
    }

    @Override
    public Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure {
        return nodeRegistryLogAggregationFacade.getLogLastAggregated(nodeIdentifier);
    }

    @Override
    public void setAggregateLogs(NodeReference nodeIdentifier, Boolean aggregateLogs) throws ServiceFailure {
        nodeRegistryLogAggregationFacade.setAggregateLogs(nodeIdentifier, aggregateLogs);
    }

    @Override
    public void setLogLastAggregated(NodeReference nodeIdentifier, Date logAggregationDate) throws ServiceFailure {
        nodeRegistryLogAggregationFacade.setLogLastAggregated(nodeIdentifier, logAggregationDate);
    }
    
}
