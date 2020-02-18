package io.boca.response;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.Test;
import java.util.List;

import static org.apache.http.client.protocol.HttpClientContext.COOKIE_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.http.util.EntityUtils;

public class ResponseTester {

  @Test
  public void testResponse() throws Exception {
    HttpPost httpPost = null;
    HttpGet httpGet = null;
    try {
      CookieStore cookieStore = new BasicCookieStore();
      HttpContext httpContext = new BasicHttpContext();
      httpContext.setAttribute(COOKIE_STORE, cookieStore);
      String loginUrl = "http://localhost:9090/api/login";
      HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();
      httpPost = new HttpPost(loginUrl);
      httpPost.addHeader("content-type", "application/json;charset=UTF-8");
      StringEntity data = new StringEntity("{\"username\":\"app\",\"password\":\"app\"}");
      httpPost.addHeader("User-Agent", "Apache HTTPClient");

      httpPost.setEntity(data);
      HttpResponse response = client.execute(httpPost, httpContext);

      HttpEntity entity = response.getEntity();

      String content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println(content);
      System.out.println("\n\n");

      // Test schema fetch
      String schemaUrl = "http://localhost:9090/api/schema";
      httpPost = new HttpPost(schemaUrl);
      httpPost.addHeader("content-type", "application/json;charset=UTF-8");
      data = new StringEntity("{\"tablename\":\"test\"}");
      httpPost.addHeader("User-Agent", "Apache HTTPClient");

      httpPost.setEntity(data);
      response = client.execute(httpPost, httpContext);

      entity = response.getEntity();
      content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println(content);
      System.out.println("\n\n");

      // Test sampleRows fetch
      String sampleUrl = "http://localhost:9090/api/sampleRows";
      httpPost = new HttpPost(sampleUrl);
      httpPost.addHeader("content-type", "application/json;charset=UTF-8");
      data = new StringEntity("{\"tablename\":\"test\"}");
      httpPost.addHeader("User-Agent", "Apache HTTPClient");

      httpPost.setEntity(data);
      response = client.execute(httpPost, httpContext);

      entity = response.getEntity();
      content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println(content);
      System.out.println("\n\n");


      // Test refreshTables fetch
      String refreshUrl = "http://localhost:9090/api/refreshTables";
      httpGet = new HttpGet(refreshUrl);
      httpGet.addHeader("content-type", "application/json;charset=UTF-8");
      httpGet.addHeader("User-Agent", "Apache HTTPClient");

      response = client.execute(httpGet, httpContext);

      entity = response.getEntity();
      content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println(content);
      System.out.println("\n\n");



    } finally {

      if (httpPost != null) {

        httpPost.releaseConnection();
      }
    }
  }
}
