package macrobase.runtime.resources;

import io.boca.internal.tables.DependencyData;
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
      TableData td = TableManager.getTableData(fir.workflowid);
      response.kpidata = new ArrayList<>(fir.kpicols.size());
      for(String kpiCol: fir.kpicols) {
        DependencyData dd = td.getDependencyData(kpiCol, ingester);
        FastInsightResponse.KpiData kpid = new FastInsightResponse.KpiData();
        kpid.kpicolname = kpiCol;
        kpid.kpitype = dd.getKpiColFeatureType().name();
        Map<String, Double> pearsonMap = dd.getPearsonFeatureMap();
        Map<String, Double> chiSquareMap = dd.getChisquareFeatureMap();
        Map<String, Double> anovaMap = dd.getAnovaFeatureMap();
        if (!pearsonMap.isEmpty()) {
          kpid.pearsonfeatures = pearsonMap.entrySet().stream().map(entry ->
              new FastInsightResponse.KpiData.PredictorData(entry.getKey(),
                  entry.getValue())).collect(Collectors.toList());
        }

        if (!chiSquareMap.isEmpty()) {
          kpid.chisquarefeatures = chiSquareMap.entrySet().stream().map(entry ->
              new FastInsightResponse.KpiData.PredictorData(entry.getKey(),
                  entry.getValue())).collect(Collectors.toList());
        }

        if (!anovaMap.isEmpty()) {
          kpid.anovafeatures = anovaMap.entrySet().stream().map(entry ->
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

