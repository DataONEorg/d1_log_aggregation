/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.solr.client.solrj.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.*;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.dataone.client.auth.CertificateManager;
import org.dataone.configuration.Settings;
import org.jsslutils.extra.apachehttpclient.SslContextedSecureProtocolSocketFactory;
import org.jsslutils.sslcontext.SSLContextFactory.SSLContextFactoryException;

/**
 *
 * @author waltz
 */
public class CommonsHttpClientProtocolRegistry {
    private static Log log = LogFactory.getLog(CommonsHttpClientProtocolRegistry.class);
    private static String keyStorePassword = Settings.getConfiguration().getString("certificate.keystore.password");
    private static String keyStoreType = Settings.getConfiguration().getString("certificate.keystore.type", KeyStore.getDefaultType());
    private static String clientCertificateLocation =
            Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");
    private static boolean useDefaultTruststore = Settings.getConfiguration().getBoolean("certificate.truststore.useDefault", true);
    private static CommonsHttpClientProtocolRegistry commonsHttpClientProtocolRegistry;
    public static CommonsHttpClientProtocolRegistry createInstance() throws SSLContextFactoryException, NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, KeyManagementException, CertificateException, IOException  {
        if (commonsHttpClientProtocolRegistry == null) {
            commonsHttpClientProtocolRegistry = new CommonsHttpClientProtocolRegistry();
                        // Using SslContextedSecureProtocolSocketFactory
            // This doesn't depend on the rest of jSSLutils and could use any
            // other SSLContext.
            SslContextedSecureProtocolSocketFactory secureProtocolSocketFactory =
                    new SslContextedSecureProtocolSocketFactory( getSslClientContext(), false);

            Protocol.registerProtocol("https", new Protocol("https",
                    (ProtocolSocketFactory) secureProtocolSocketFactory, 443));

        }
        return commonsHttpClientProtocolRegistry;
    }



    /**
     *
     * @return
     */
    private static SSLContext getSslClientContext() throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, KeyManagementException, CertificateException, IOException {

        SSLContext ctx = SSLContext.getInstance("TLS");
        // only connecting to the localhost, so we should be able to trust ourselves
        X509TrustManager tm = getTrustManager();
        KeyStore keyStore = null;

        // get the keystore that will provide the material
        // Catch the exception here so that the TLS connection scheme
        // will still be setup if the client certificate is not found.
        try {
            keyStore = getKeyStore();
        } catch (FileNotFoundException e) {
            // these are somewhat expected for anonymous d1 client use
            log.warn("Client certificate could not be located. Setting up SocketFactory without it." + e.getMessage());
        }
        // specify the client key manager
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        // initialize the context
        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
        return ctx;
    }

    /**
     * Based on configuration option, 'certificate.truststore.useDefault', returns either the 
     * default Java truststore or the allow-all implementation.
     * @return X509TrustManager for verifying server identity
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     */
    private static X509TrustManager  getTrustManager() throws NoSuchAlgorithmException, KeyStoreException {
    	
    	if (useDefaultTruststore) {
    		// this is the Java truststore and is administered outside of the code
	    	TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());  
		    trustManagerFactory.init((KeyStore) null);  
		      
		    log.debug("JVM Default Trust Managers:");  
		    for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {  
		        log.debug(trustManager);  
		        if (trustManager instanceof X509TrustManager) {  
		            X509TrustManager x509TrustManager = (X509TrustManager) trustManager;  
		            log.debug("Accepted issuers count : " + x509TrustManager.getAcceptedIssuers().length);
		            //for (X509Certificate issuer: x509TrustManager.getAcceptedIssuers()) {
		            //	log.debug("trusted issuer: " + issuer.getSubjectDN().toString());
		            //}
		            return x509TrustManager;
		        }  
		    }
		    return null;
    	} else {
	    
		    // otherwise we return a very liberal one
	        X509TrustManager tm = new X509TrustManager() {
	            public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
	            	log.debug("checkClientTrusted - " + string);
	            }
	            public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
	            	log.debug("checkServerTrusted - " + string);
	            }
	            public X509Certificate[] getAcceptedIssuers() {
	            	log.debug("getAcceptedIssuers");
	            	return null;
	            }
	        };
	    
	        return tm;
    	}
    }
    /**
     * Loads the certificate and privateKey into the in-memory KeyStore singleton, using the provided subjectString to
     * search among the registered certificates. If the subjectString parameter is null, finds the certificate first
     * using the set certificate location, and then the default location.
     *
     *
     * NOTE: this implementation uses Bouncy Castle security provider
     *
     * @param subjectString - key for the registered certificate to load into the keystore an unregistered subjectString
     * will lead to a KeyStoreException ("Cannot store non-PrivateKeys")
     * @return the keystore singleton instance that will provide the material
     * @throws KeyStoreException
     * @throws CertificateException
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    private static KeyStore getKeyStore() throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {

        // the important items
        X509Certificate certificate = null;
        PrivateKey privateKey = null;

        // get the private key and certificate from the PEM
        // TODO: find a way to do this with default Java provider (not Bouncy Castle)?
        Security.addProvider(new BouncyCastleProvider());
        PEMReader pemReader = new PEMReader(new FileReader(clientCertificateLocation));
        Object pemObject = null;

        KeyPair keyPair = null;
        while ((pemObject = pemReader.readObject()) != null) {
            if (pemObject instanceof PrivateKey) {
                privateKey = (PrivateKey) pemObject;
            } else if (pemObject instanceof KeyPair) {
                keyPair = (KeyPair) pemObject;
                privateKey = keyPair.getPrivate();
            } else if (pemObject instanceof X509Certificate) {
                certificate = (X509Certificate) pemObject;
            }
        }

        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, keyStorePassword.toCharArray());
        java.security.cert.Certificate[] chain = new java.security.cert.Certificate[]{certificate};
        // set the entry
        keyStore.setKeyEntry("dataone", privateKey, keyStorePassword.toCharArray(), chain);

        return keyStore;

    }
}
