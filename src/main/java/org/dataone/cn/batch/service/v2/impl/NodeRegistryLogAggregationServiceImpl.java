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
 *
 * @author waltz
 */
public class NodeRegistryLogAggregationServiceImpl implements NodeRegistryLogAggregationService {

                private NodeRegistryLogAggregationFacade nodeRegistrySyncFacade
                    = new NodeRegistryLogAggregationFacade();
    @Override
    public NodeList listNodes() throws ServiceFailure, NotImplemented {
        return nodeRegistrySyncFacade.getApprovedNodeList();
    }

    @Override
    public Node getNode(NodeReference nodeId) throws NotFound, ServiceFailure {
        return nodeRegistrySyncFacade.getNode(nodeId);
    }

    @Override
    public Boolean getAggregateLogs(NodeReference nodeIdentifier) throws ServiceFailure {
        return nodeRegistrySyncFacade.getAggregateLogs(nodeIdentifier);
    }

    @Override
    public Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setAggregateLogs(NodeReference nodeIdentifier, Boolean aggregateLogs) throws ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setLogLastAggregated(NodeReference nodeIdentifier, Date logAggregationDate) throws ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
