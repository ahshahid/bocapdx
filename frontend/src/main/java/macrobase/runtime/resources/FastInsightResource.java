package macrobase.runtime.resources;

import io.boca.internal.tables.DependencyData;
import io.boca.internal.tables.FeatureType;
import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.SQLIngester;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/fastInsight")
@Produces(MediaType.APPLICATION_JSON)

public class FastInsightResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(FastInsightResource.class);
  private static double pearsonCorrCriteria = .5d;
  private static double annovaCorrCriteria = .5d;
  private static double chiCriteria = .1d;

  @Context
  private HttpServletRequest request;

  static class FastInsightRequest {
    public int workflowid;
    public List<String> kpicols;
  }

  static class FastInsightResponse {
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





  public FastInsightResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public FastInsightResponse getSchema(FastInsightRequest fir) {
    FastInsightResponse response = new FastInsightResponse();
    HttpSession ss = request.getSession();
    try {
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);

      final TableData tdOrig = TableManager.getTableData(fir.workflowid);
      TableData currentTd = tdOrig;

      response.kpidata = new ArrayList<>(fir.kpicols.size());
      for(String kpiCol: fir.kpicols) {
        TableData.ColumnData kpiColData = tdOrig.getColumnData(kpiCol);
        if (!kpiColData.skip && kpiColData.ft.equals(FeatureType.categorical)) {
          currentTd = tdOrig.getTableDataForFastInsights();
        } else {
          currentTd = tdOrig;
        }
        DependencyData dd = currentTd.getDependencyData(kpiCol, ingester);
        FastInsightResponse.KpiData kpid = new FastInsightResponse.KpiData();
        kpid.kpicolname = kpiCol;
        kpid.kpitype = dd.getKpiColFeatureType().name();
        Map<String, Double> pearsonMap = dd.getPearsonFeatureMap();
        Map<String, Double> chiSquareMap = dd.getChisquareFeatureMap();
        Map<String, Double> anovaMap = dd.getAnovaFeatureMap();
        if (!pearsonMap.isEmpty()) {
          kpid.pearsonfeatures = pearsonMap.entrySet().stream().filter(entry ->
              Math.abs(entry.getValue()) > pearsonCorrCriteria).sorted((e1, e2) ->
              e2.getValue().compareTo(e1.getValue())).map(entry ->
              new FastInsightResponse.KpiData.PredictorData(entry.getKey(),
                  entry.getValue())).collect(Collectors.toList());
        }

        if (!chiSquareMap.isEmpty()) {
          kpid.chisquarefeatures = chiSquareMap.entrySet().stream().filter(entry ->
              entry.getValue()< chiCriteria ).sorted((e1, e2) ->
              e1.getValue().compareTo(e2.getValue())).map(entry ->
              new FastInsightResponse.KpiData.PredictorData(entry.getKey(),
                  entry.getValue())).collect(Collectors.toList());
        }

        if (!anovaMap.isEmpty()) {
          kpid.anovafeatures = anovaMap.entrySet().stream().filter(entry ->
              entry.getValue() > annovaCorrCriteria ).sorted((e1, e2) ->
              e2.getValue().compareTo(e1.getValue())).map(entry ->
              new FastInsightResponse.KpiData.PredictorData(entry.getKey(),
                  entry.getValue())).collect(Collectors.toList());
        }
        response.kpidata.add(kpid);
      }

    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }
}

