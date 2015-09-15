/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.v1;

import org.dataone.cn.batch.logging.v1.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dataone.client.auth.ClientIdentityManager;
import org.dataone.client.auth.X509Session;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.HttpMultipartRestClient;
import org.dataone.client.utils.HttpUtils;
import org.dataone.client.v1.MNode;
import org.dataone.client.v1.impl.MultipartMNode;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Node;

/**
 * Manage the D1 client connections
 *
 * The class will pool the connections. Each new node will receive a client and the client is re-used for future calls.
 *
 * The
 *
 * @author waltz
 */
public class ClientMNodeService {
    
    private Map<NodeReference, MNode> clientPool = new HashMap<>();
    static ClientMNodeService nodeClientSingleton = null;
    private static final String CN_METACAT_LOG_V1_PATH = "/metacat/d1/cn/v1/log";
    NodeRegistryService nodeRegistryService = new NodeRegistryService();
    
    private ClientMNodeService() {

    }

    public static ClientMNodeService getInstance() {
        if (nodeClientSingleton == null) {
            nodeClientSingleton = new ClientMNodeService();
        }
        return nodeClientSingleton;

    }

    public MNode getClientMNode(NodeReference mnNodeReference)  {
        MNode mNode = null;
        if (clientPool.containsKey(mnNodeReference)) {
            mNode = clientPool.get(mnNodeReference);
        } else {
            try {
                
                Node node = nodeRegistryService.getApprovedNode(mnNodeReference);
                
                HttpMultipartRestClient multipartRestClient = new HttpMultipartRestClient();
                if (node.getType().compareTo(NodeType.MN) == 0) {
                    mNode = new MultipartMNode (multipartRestClient, node.getBaseURL());
                } else {
                    //
                    // The CN must be contacted through the metacat MN interface in order to 
                    // retrieve the log records
                    //

                    String cnBaseUrl = node.getBaseURL();
                    StringBuilder cnMetacatLogUrl = new StringBuilder(cnBaseUrl
                                            .substring(0, cnBaseUrl.lastIndexOf("/cn")));
                    cnMetacatLogUrl.append(CN_METACAT_LOG_V1_PATH);
                    mNode = new MultipartMNode (multipartRestClient, cnMetacatLogUrl.toString());
                }
            } catch (ServiceFailure ex) {
                Logger.getLogger(ClientMNodeService.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NotFound ex) {
                Logger.getLogger(ClientMNodeService.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                
            } catch (ClientSideException ex) {
                Logger.getLogger(ClientMNodeService.class.getName()).log(Level.SEVERE, null, ex);
            }
            clientPool.put(mnNodeReference, mNode);
            
            
        }
        return mNode;
        
    }

}
