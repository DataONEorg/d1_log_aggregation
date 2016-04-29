/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap;

import java.util.Date;
import java.util.Map;
import javax.naming.directory.DirContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;

/**
 * Extend the NodeFacade class in d1_cn_noderegistery to allow
 * for log aggregation specific behavior
 * 
 * Expose public access to protected methods in NodeAccess
 *
 * The public methods will also control the borrowing and returning of LDAPContexts to the LDAP Pool
 *
 * @author waltz
 */
public class NodeRegistryLogAggregationFacade extends NodeFacade {

    public static Log log = LogFactory.getLog(NodeRegistryLogAggregationFacade.class);


    public Date getLogLastAggregated(NodeReference nodeIdentifier) throws ServiceFailure {
        DirContext dirContext = null;
        Date logLastAggregated;
        try {
            dirContext = getDirContextProvider().borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("16805", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("16805", "Context is null.Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            logLastAggregated = getNodeAccess().getLogLastAggregated(dirContext, nodeIdentifier);
        } finally {
            getDirContextProvider().returnDirContext(dirContext);
        }
        return logLastAggregated;
    }


    public void setLogLastAggregated(NodeReference nodeIdentifier, Date logAggregationDate) throws ServiceFailure {
        DirContext dirContext = null;
        try {
            dirContext = getDirContextProvider().borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("16803", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("16803", "Context is null.Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            getNodeAccess().setLogLastAggregated(dirContext, nodeIdentifier, logAggregationDate);
        } finally {
            getDirContextProvider().returnDirContext(dirContext);
        }
    }


    public Boolean getAggregateLogs(NodeReference nodeIdentifier) throws ServiceFailure {
        DirContext dirContext = null;
        Boolean aggregateLogs = null;
        try {
            dirContext = getDirContextProvider().borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("16806", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("16806", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            aggregateLogs = getNodeAccess().getAggregateLogs(dirContext, nodeIdentifier);
        } finally {
            getDirContextProvider().returnDirContext(dirContext);
        }
        return aggregateLogs;
    }


    public void setAggregateLogs(NodeReference nodeIdentifier, Boolean aggregateLogs) throws ServiceFailure {
        DirContext dirContext = null;
        try {
            dirContext = getDirContextProvider().borrowDirContext();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceFailure("16804", ex.getMessage());
        }
        if (dirContext == null) {
            throw new ServiceFailure("16804", "Context is null. Unable to retrieve LDAP Directory Context from pool. Please try again.");
        }
        try {
            getNodeAccess().setAggregateLogs(dirContext, nodeIdentifier, aggregateLogs);
        } finally {
            getDirContextProvider().returnDirContext(dirContext);
        }
    }

}
