package io.boca.response;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
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

import java.util.Iterator;
import java.util.List;

import static org.apache.http.client.protocol.HttpClientContext.COOKIE_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.http.util.EntityUtils;

public class ResponseTester {
  String host = "localhost";
  @Test
  public void testResponse() throws Exception {
    HttpPost httpPost = null;
    HttpGet httpGet = null;
    try {
      CookieStore cookieStore = new BasicCookieStore();
      HttpContext httpContext = new BasicHttpContext();
      httpContext.setAttribute(COOKIE_STORE, cookieStore);
      String loginUrl = "http://" + host + ":9090/api/login";
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


      ObjectMapper objectMapper = new ObjectMapper();
       String tableName = "airline_ext";

        // Test schema fetch
        String schemaUrl = "http://" + host + ":9090/api/schema";
        httpPost = new HttpPost(schemaUrl);
        httpPost.addHeader("content-type", "application/json;charset=UTF-8");
        data = new StringEntity("{\"table\":{\"name\":\"" +  tableName + "\"}}");
        httpPost.addHeader("User-Agent", "Apache HTTPClient");

        httpPost.setEntity(data);
        response = client.execute(httpPost, httpContext);

        entity = response.getEntity();
        content = EntityUtils.toString(entity);
        System.out.println("\n\n");
        System.out.println("for table = " + tableName);
        System.out.println(content);
        System.out.println("\n\n");


      JsonNode rootNode = objectMapper.readTree(content);
      int  workflowid = ((IntNode)rootNode.get("workflowid")).intValue();



      // Test sampleRows fetch
      String sampleUrl = "http://" + host + ":9090/api/sampleRows";
      httpPost = new HttpPost(sampleUrl);
      httpPost.addHeader("content-type", "application/json;charset=UTF-8");
      data = new StringEntity("{\"workflowid\":"+ workflowid +"}");
      httpPost.addHeader("User-Agent", "Apache HTTPClient");

      httpPost.setEntity(data);
      response = client.execute(httpPost, httpContext);

      entity = response.getEntity();
      content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println(content);
      System.out.println("\n\n");


      // Test refreshTables fetch
      String refreshUrl = "http://" + host + ":9090/api/refreshTables";
      httpGet = new HttpGet(refreshUrl);
      httpGet.addHeader("content-type", "application/json;charset=UTF-8");
      httpGet.addHeader("User-Agent", "Apache HTTPClient");

      response = client.execute(httpGet, httpContext);

      entity = response.getEntity();
      content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println(content);
      System.out.println("\n\n");


      // dependency fetch
     String fastInsightUrl = "http://" + host + ":9090/api/fastInsight";
      httpPost = new HttpPost(fastInsightUrl);
      httpPost.addHeader("content-type", "application/json;charset=UTF-8");
      data = new StringEntity("{\"workflowid\":" + workflowid +", \"kpicols\":[\"weatherdelay\"]}");
      httpPost.addHeader("User-Agent", "Apache HTTPClient");

      httpPost.setEntity(data);
      response = client.execute(httpPost, httpContext);

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