package com.bianzx.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @ClassName: JestHttpClientFactory
 * @Description: Jest HttpClient Factory
 * @author: bianzexin
 * @date: Nov 2, 2020
 *
 */
public class JestHttpClientFactory {

  private JestHttpClientFactory() {

  }

  /**
   * 获取JestClient
   * @param host
   * @param discovery
   * @return
   * @throws Throwable
   */
  public static JestClient getClient(final String host, final Boolean discovery) throws Throwable {
    JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(config(host, discovery));
    return factory.getObject();
  }

  /**
   * 配置JestClient的http config
   * @param host
   * @param discovery
   * @return
   * @throws GeneralSecurityException
   */
  private static HttpClientConfig config(String host, Boolean discovery)
      throws GeneralSecurityException {
    HttpClientConfig.Builder clientConfig = new HttpClientConfig.Builder(uris(host))
        .multiThreaded(true).defaultSchemeForDiscoveredNodes(defaultSchema(host))
        .sslSocketFactory(getSyncHttpsHandler()).httpsIOSessionStrategy(getAsyncHttpsHandler());

    if (discovery) {
      clientConfig.discoveryFrequency(1L, TimeUnit.MINUTES).discoveryEnabled(true);
    }

    return clientConfig.build();
  }

  /**
   * 解析hosts
   * @param host
   * @return
   */
  private static Collection<String> uris(String host) {
    final String[] uris = host.split(",");
    return Arrays.asList(uris);
  }

  private static String defaultSchema(String host) {
    return host.substring(0, host.indexOf("://"));
  }

  private static SSLConnectionSocketFactory getSyncHttpsHandler() throws GeneralSecurityException {
    return new SSLConnectionSocketFactory(getSslContext(), NoopHostnameVerifier.INSTANCE);
  }

  private static SchemeIOSessionStrategy getAsyncHttpsHandler() throws GeneralSecurityException {
    return new SSLIOSessionStrategy(getSslContext(), NoopHostnameVerifier.INSTANCE);
  }

  private static SSLContext getSslContext() throws GeneralSecurityException {
    return new SSLContextBuilder().loadTrustMaterial(null, new TrustEverythingStrategy()).build();
  }

  private static class TrustEverythingStrategy implements TrustStrategy {
    @Override
    public boolean isTrusted(X509Certificate[] x509Certificates, String s)
        throws CertificateException {
      return true;
    }
  }
}
