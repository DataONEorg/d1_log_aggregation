/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.client.rest;

import java.io.InputStream;
import org.apache.http.Header;
import org.dataone.client.auth.X509Session;
import org.dataone.client.exception.ClientSideException;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;

/**
 *
 * @author waltz
 */
public class MockMultipartRestClient implements MultipartRestClient {

    @Override
    public InputStream doGetRequest(String url, Integer timeoutMillisecs) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream doGetRequest(String url, Integer timeoutMillisecs, boolean followRedirect) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Header[] doGetRequestForHeaders(String url, Integer timeoutMillisecs) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream doDeleteRequest(String url, Integer timeoutMillisecs) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Header[] doHeadRequest(String url, Integer timeoutMillisecs) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream doPutRequest(String url, SimpleMultipartEntity entity, Integer timeoutMillisecs) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream doPostRequest(String url, SimpleMultipartEntity entity, Integer timeoutMillisecs) throws BaseException, ClientSideException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getLatestRequestUrl() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public X509Session getSession() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
