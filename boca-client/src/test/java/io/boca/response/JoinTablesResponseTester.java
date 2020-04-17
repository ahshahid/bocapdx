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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.http.client.protocol.HttpClientContext.COOKIE_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.http.util.EntityUtils;

public class JoinTablesResponseTester {
  String host = "localhost";
  @Test
  public void testResponse() throws Exception {
    HttpPost httpPost = null;
    HttpGet httpGet = null;
    final String kpiCol = "Churn";//"telecom_churn_billing_churn";
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
      String table1 = "telecom_churn_billing";
      String table2 = "telecom_churn_networkq";

      // Test schema fetch
      String schemaUrl = "http://" + host + ":9090/api/schema";
      httpPost = new HttpPost(schemaUrl);
      httpPost.addHeader("content-type", "application/json;charset=UTF-8");
     // data = new StringEntity("{\"table\":{\"name\":\"" +  table1 + "\", \"joinlist\":[{\"table\":{\"name\":\"" + table2 +"\"}}]}}");
      data = new StringEntity("{\"table\":{\"name\":\"" +  table1 + "\"}}");

      httpPost.addHeader("User-Agent", "Apache HTTPClient");

      httpPost.setEntity(data);
      response = client.execute(httpPost, httpContext);

      entity = response.getEntity();
      content = EntityUtils.toString(entity);
      System.out.println("\n\n");
      System.out.println("for table = " + table1 + "join " +table2);
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




      String [] dependentCols = fastInsightFetch(workflowid, client, httpContext, kpiCol);

      deepInsightFetch(workflowid, client, httpContext, kpiCol, dependentCols);
     // graphFetch(workflowid, client, httpContext, "avgrev", "avgmou", 0);



    } finally {

      if (httpPost != null) {

        httpPost.releaseConnection();
      }
    }
  }

  private String[] fastInsightFetch(int workflowid, HttpClient client, HttpContext httpContext, String kpiCol)
          throws Exception {
    // dependency fetch
    String fastInsightUrl = "http://" + host + ":9090/api/fastInsight";
    HttpPost httpPost = new HttpPost(fastInsightUrl);
    httpPost.addHeader("content-type", "application/json;charset=UTF-8");
    StringEntity data = new StringEntity("{\"workflowid\":" + workflowid +", \"kpicols\":[\""+ kpiCol +"\"]}");
    httpPost.addHeader("User-Agent", "Apache HTTPClient");

    httpPost.setEntity(data);
    HttpResponse response = client.execute(httpPost, httpContext);

    HttpEntity entity = response.getEntity();
    String content = EntityUtils.toString(entity);
    System.out.println("\n\n");
    System.out.println(content);
    System.out.println("\n\n");

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(content);
    JsonNode elem =  ((ArrayNode)rootNode.get("kpidata")).get(0);
    List<String> dependentCols = new ArrayList<>();
    JsonNode nod = elem.get("pearsonfeatures");
    if (!nod.isNull()) {
      ArrayNode arr = (ArrayNode)nod;
      for( int i = 0; i < arr.size(); ++i) {
        JsonNode each = arr.get(i);
        dependentCols.add(each.get("predictorname").textValue());
      }
    }
    nod = elem.get("chisquarefeatures");
    if (!nod.isNull()) {
      ArrayNode arr = (ArrayNode)nod;
      for( int i = 0; i < arr.size(); ++i) {
        JsonNode each = arr.get(i);
        dependentCols.add(each.get("predictorname").textValue());
      }
    }
    nod = elem.get("anovafeatures");
    if (!nod.isNull()) {
      ArrayNode arr = (ArrayNode)nod;
      for( int i = 0; i < arr.size(); ++i) {
        JsonNode each = arr.get(i);
        dependentCols.add(each.get("predictorname").textValue());
      }
    }
    return dependentCols.toArray(new String[dependentCols.size()]);

  }
  private void graphFetch(int workflowid, HttpClient client, HttpContext httpContext, String metric, String feature, int graphFor) throws Exception {
    //
    String graphUrl = "http://" + host + ":9090/api/graph";
    HttpPost httpPost = new HttpPost(graphUrl);
    httpPost.addHeader("content-type", "application/json;charset=UTF-8");
    StringEntity data = new StringEntity("{\"workflowid\":" + workflowid +", \"metric\":\"" +metric + "\", \"feature\":\""+ feature +"\"," +
        "\"graphfor\":" + graphFor+"}"
    );
    httpPost.addHeader("User-Agent", "Apache HTTPClient");

    httpPost.setEntity(data);
    HttpResponse response = client.execute(httpPost, httpContext);

    HttpEntity entity = response.getEntity();
    String content = EntityUtils.toString(entity);
    System.out.println("\n\n");
    System.out.println(content);
    System.out.println("\n\n");
  }

  private void deepInsightFetch(int workflowid, HttpClient client,
                                HttpContext httpContext, String kpiCol, String[] dependents) throws Exception {
    // deep insifgt fetch
    String attribs = Arrays.stream(dependents).map(str -> "\"" + str + "\"").
            collect(Collectors.joining(","));
    String deepInsightUrl = "http://" + host + ":9090/api/deepInsight";
    HttpPost httpPost = new HttpPost(deepInsightUrl);
    httpPost.addHeader("content-type", "application/json;charset=UTF-8");
    StringEntity data = new StringEntity("{\"workflowid\":" + workflowid +", \"metric\":\"" + kpiCol +"\", \"objective\":\"xxx\"," +
        "\"optionalConf\":" +
        "{" +
             "\"attributes\":["+attribs +"]" +
       // "\"predicate\":\"telecom_churn_networkq_churn = 0\"," +
       // "\"minSupport\": 0.0001" +
        "}" +
        "}"
    );
    httpPost.addHeader("User-Agent", "Apache HTTPClient");

    httpPost.setEntity(data);
    HttpResponse response = client.execute(httpPost, httpContext);

    HttpEntity entity = response.getEntity();
    String content = EntityUtils.toString(entity);
    System.out.println("\n\n");
    System.out.println(content);
    System.out.println("\n\n");
    generateChart(content);
  }

  private void generateChart(String deepResp)  throws Exception {
    String fileName = "chart.template";
    ClassLoader classLoader = getClass().getClassLoader();

    File file = new File(classLoader.getResource(fileName).getFile());


    //Read File Content
    String content = new String(Files.readAllBytes(file.toPath()));

     ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(deepResp);
    ArrayNode arr =  (ArrayNode)rootNode.get("expl").get("nlgExplanation");
    List<String> graphs = new ArrayList<>();
    for( int i = 0; i < 1; ++i) {
      JsonNode eachExpl = arr.get(i);
      ArrayNode arrg = (ArrayNode) eachExpl.get("graphs");
      for( int j = 0; j < 1; ++j) {
        JsonNode node = arrg.get(j);
        String temp = node.toString();
        temp = temp.substring(1);
        temp = temp.substring(0, temp.length() -1);
        graphs.add(temp);
      }
    }
    String subst1 = graphs.stream().collect(Collectors.joining(","));
    StringBuilder temp= new StringBuilder();
    for(int j =0 ; j < graphs.size(); ++j) {
      temp.append("<div id=").append(j).append("></div>\n");
    }
    String subst2 = temp.toString();
    String newHtml = String.format(content, subst1, subst2);
    File output = new File("charts.html");
    FileOutputStream fos = new FileOutputStream(output);
    fos.write(newHtml.getBytes());
    fos.flush();
  }
}
