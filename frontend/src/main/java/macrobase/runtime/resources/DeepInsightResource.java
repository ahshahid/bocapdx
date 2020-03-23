package macrobase.runtime.resources;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.pipeline.BasicBatchPipeline;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import io.boca.internal.tables.DependencyData;
import io.boca.internal.tables.FeatureType;
import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;
import macrobase.analysis.pipeline.BasicBatchedPipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/deepInsight")
@Produces(MediaType.APPLICATION_JSON)

public class DeepInsightResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(FastInsightResource.class);

  @Context
  private HttpServletRequest request;

  static class DeepInsightRequest {
    public int workflowid;
    public String metric;  //outlier column
    public String objective;
    public Map<String, Object> optionalConf;
  }

  static class DeepInsightResponse {

    public String errorMessage;
    public Explanation expl = null;
    private DeepInsightResponse() {}
    private DeepInsightResponse (Explanation expl) {
      this.expl = expl;
    }

  }


  public DeepInsightResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public DeepInsightResponse getSchema(DeepInsightRequest dir) {
    DeepInsightResponse response ;
    HttpSession ss = request.getSession();
    try {
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);

      final TableData tdOrig = TableManager.getTableData(dir.workflowid);
      final TableData preppedTableData = tdOrig.getTableDataForFastInsights();

      List<Schema.SchemaColumn> actualCols = preppedTableData.getSchema().getColumns().stream().
          filter(sc -> sc.getName().
              equalsIgnoreCase(MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX + dir.metric)).
          collect(Collectors.toList());
      String metricCol = dir.metric;
      if (!actualCols.isEmpty()) {
        metricCol = actualCols.get(0).getName();
      }
      String tableName = preppedTableData.getTableOrView();
      // validate optionalConf
      Map<String, Object> optionalConf = dir.optionalConf;
      if (optionalConf != null) {
        // validate keys
        optionalConf.keySet().stream().forEach(key -> {
          boolean found = false;
          for(String param: MacroBaseConf.optionalParams) {
            if (key.equals(param)) {
              found = true;
              break;
            }
          }
          if (!found) {
            throw new RuntimeException("Unknown optional param = " + key);
          }
        });
      } else {
        optionalConf = new HashMap<>();
      }
      optionalConf.put(MacroBaseConf.BASE_TABLE_KEY, tableName);
      optionalConf.put(MacroBaseConf.METRIC_KEY, metricCol);
      optionalConf.put(MacroBaseConf.PROVIDED_CONN_KEY, ingester.getConnection());
      optionalConf.put(MacroBaseConf.SUMMARIZER_KEY, "apriori");
      if (optionalConf.containsKey(MacroBaseConf.EXTRA_PRED_KEY)) {
        String extraPred = (String)optionalConf.get(MacroBaseConf.EXTRA_PRED_KEY);
        if (extraPred != null && !extraPred.trim().isEmpty()) {
          String query = "select * from " + tableName + " " + extraPred;
          Statements stmt = CCJSqlParserUtil.parseStatements(query);
          System.out.println(stmt);

        }

      }


      PipelineConfig config = new PipelineConfig(optionalConf);
      BasicBatchPipeline bbp = new BasicBatchPipeline(config);
      Explanation expl = bbp.results();
      response = new DeepInsightResponse(expl);

    } catch (Exception e) {
      response = new DeepInsightResponse();
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }
}

