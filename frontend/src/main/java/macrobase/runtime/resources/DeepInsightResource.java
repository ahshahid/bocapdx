package macrobase.runtime.resources;

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
    public String classifier;  // percentile, predicate etc
    public String objective;
    public String extraPredicate;
    public String predicate;
    public String ratioMetric;
    /// numeric data data
    public String cutoff;
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
      conf.put(MacroBaseConf.BASE_TABLE_KEY, tableName);
      if (dir.extraPredicate != null && !dir.extraPredicate.trim().isEmpty()) {
        conf.put(MacroBaseConf.EXTRA_PRED_KEY, dir.extraPredicate);
      }
      if (dir.classifier != null && !dir.classifier.trim().isEmpty()) {
        conf.put(MacroBaseConf.CLASSIFIER_KEY, dir.classifier);
      }
      conf.put(MacroBaseConf.METRIC_KEY, metricCol);
      if (dir.predicate != null && !dir.predicate.trim().isEmpty()) {
        conf.put(MacroBaseConf.PRED_KEY, dir.predicate);
      }

      if (dir.attributes != null && !dir.attributes.isEmpty()) {
        conf.put(MacroBaseConf.ATTRIBUTES_KEY, dir.attributes);
      }
      conf.put(MacroBaseConf.PROVIDED_CONN_KEY, ingester.getConnection());

      if (dir.cutoff != null && !dir.cutoff.trim().isEmpty()) {
        conf.put(MacroBaseConf.CUT_OFF_KEY, Double.parseDouble(dir.cutoff));
      }
      if (dir.ratioMetric != null && !dir.ratioMetric.trim().isEmpty()) {
        conf.put(MacroBaseConf.RATIO_METRIC_KEY, dir.ratioMetric);
      }

      if (dir.minRatioMetric != null && !dir.minRatioMetric.trim().isEmpty()) {
        conf.put(MacroBaseConf.MIN_RATIO_METRIC_KEY, Double.parseDouble(dir.minRatioMetric));
      }

      if (dir.minSupport != null && !dir.minSupport.trim().isEmpty()) {
        conf.put(MacroBaseConf.MIN_SUPPORT_KEY, Double.parseDouble(dir.minSupport));
      }

      if (dir.maxOrder != null && !dir.maxOrder.trim().isEmpty()) {
        conf.put(MacroBaseConf.MAX_ORDER_KEY, Integer.parseInt(dir.maxOrder));
      }
      PipelineConfig config = new PipelineConfig(conf);
      BasicBatchPipeline bbp = new BasicBatchPipeline(config);


    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }
}

