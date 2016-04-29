/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging.v2;

import java.io.IOException;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.HttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.impl.MultipartMNode;
import org.dataone.cn.batch.service.v2.NodeRegistryLogAggregationService;
import org.dataone.cn.batch.service.v2.impl.NodeRegistryLogAggregationServiceImpl;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v2.Node;

/**
 * Manage the D1 client connections
 *
 * The class will pool the connections. Each new node will receive a client and the client is re-used for future calls.
 *
 * The
 *
 * @author waltz
 */
public class ClientNodeService {

    static ClientNodeService nodeClientSingleton = null;
    private static final String CN_METACAT_PATH = "/metacat/d1/cn";
    NodeRegistryLogAggregationService nodeRegistryService = new NodeRegistryLogAggregationServiceImpl();

    private ClientNodeService() {

    }

    public static ClientNodeService getInstance() {
        if (nodeClientSingleton == null) {
            nodeClientSingleton = new ClientNodeService();
        }
        return nodeClientSingleton;

    }

    public MNode getClientMNode(NodeReference mnNodeReference) {
        MNode mNode = null;

        try {

            Node node = nodeRegistryService.getNode(mnNodeReference);

            MultipartRestClient multipartRestClient = new HttpMultipartRestClient();
            if (node.getType().compareTo(NodeType.MN) == 0) {
                mNode = new MultipartMNode(multipartRestClient, node.getBaseURL());
            } else {
                    //
                // The CN must be contacted through the metacat MN interface in order to 
                // retrieve the log records
                //

                String cnBaseUrl = node.getBaseURL();
                StringBuilder cnMetacatLogUrl = new StringBuilder(cnBaseUrl
                        .substring(0, cnBaseUrl.lastIndexOf("/cn")));
                cnMetacatLogUrl.append(CN_METACAT_PATH);
                mNode = new MultipartMNode(multipartRestClient, cnMetacatLogUrl.toString());
            }

        } catch (ServiceFailure ex) {
            ex.printStackTrace();
        } catch (NotFound ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClientSideException ex) {
            ex.printStackTrace();
        }

        return mNode;

    }

}
