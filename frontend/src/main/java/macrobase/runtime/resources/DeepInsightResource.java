package macrobase.runtime.resources;

import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import io.boca.internal.tables.DependencyData;
import io.boca.internal.tables.FeatureType;
import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;
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
    public List<String> attributes;
    public String metric;  //outlier column
    public String outlierclassifier;  // percentile, predicate etc
    public String objective;
    public String cutoff;
    public String extraPredicate;
    public String ratioMetric;
    public String minRatioMetric;
    public String minSupport;
    public String maxOrder;


  }

  static class DeepInsightResponse {
    public List<KpiData> kpidata;
    public String errorMessage;
    static class KpiData {
      public String kpicolname;
      public String kpitype;
      public List<PredictorData> pearsonfeatures;
      public List<PredictorData> chisquarefeatures;
      public List<PredictorData> anovafeatures;
      static class PredictorData {
        public String predictorname;
        public double corr;
        public PredictorData(String name, double val) {
          predictorname = name;
          corr = val;
        }
      }
    }
  }





  public DeepInsightResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public DeepInsightResponse getSchema(DeepInsightRequest dir) {
    DeepInsightResponse response = new DeepInsightResponse();
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
      Map<String, Object> conf = new HashMap<>();
      conf.put()
      PipelineConfig config = new PipelineConfig();


    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }
}

