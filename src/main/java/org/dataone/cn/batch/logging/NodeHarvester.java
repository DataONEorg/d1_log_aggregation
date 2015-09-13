/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.logging;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Stack;
import org.dataone.cn.batch.logging.exceptions.QueryLimitException;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.dataone.cn.batch.logging.type.LogQueryDateRange;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;

/**
 *
 * @author waltz
 */
public interface NodeHarvester extends Serializable {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        public List<LogEntrySolrItem> harvest(Stack<LogQueryDateRange> logQueryStack, Integer queryTotalLimit) throws ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented, InvalidToken, QueryLimitException ;
        public NodeReference getNodeReference();
        public void setNodeReference(NodeReference nodeReference);
}
