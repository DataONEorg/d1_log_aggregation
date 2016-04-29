package org.dataone.cn.batch.service.v2;

import java.util.Date;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

public interface NodeRegistryLogAggregationService {

    public NodeList listNodes() 
            throws ServiceFailure, NotImplemented;
    
    public Node getNode(NodeReference nodeId) 
            throws NotFound, ServiceFailure;

    Boolean getAggregateLogs(NodeReference nodeIdentifier) throws ServiceFailure;

    Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure;

    void setAggregateLogs(NodeReference nodeIdentifier, Boolean aggregateLogs) throws ServiceFailure;

    void setLogLastAggregated(NodeReference nodeIdentifier, Date logAggregationDate) throws ServiceFailure;
    
}
