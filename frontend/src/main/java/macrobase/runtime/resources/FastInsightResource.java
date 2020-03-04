package macrobase.runtime.resources;

import io.boca.internal.tables.DependencyData;
import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/fastInsight")
@Produces(MediaType.APPLICATION_JSON)

public class FastInsightResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

  @Context
  private HttpServletRequest request;

  static class FastInsightRequest {
    public String tablename;
    public List<String> kpicols;
  }

  static class FastInsightResponse {
    public List<KpiData> kpidata;
    public String errorMessage;
    static class KpiData {
      public String kpicolname;
      public List<PredictorData> continuousfeatures;
      public List<PredictorData> categoricalfeatures;
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
      TableData td = TableManager.getTableData(fir.tablename, ingester);

      int index = 0;
      response.kpidata = new ArrayList<>(fir.kpicols.size());
      for(String kpiCol: fir.kpicols) {
        DependencyData dd = td.getDependencyData(kpiCol, ingester);
        FastInsightResponse.KpiData kpid = new FastInsightResponse.KpiData();
        kpid.kpicolname = kpiCol;
        Map<String, Double> contiMap = dd.getContinousFeatureMap();
        Map<String, Double> catMap = dd.getCategoricalFeatureMap();
        if (!contiMap.isEmpty()) {
          kpid.continuousfeatures = contiMap.entrySet().stream().map(entry ->
              new FastInsightResponse.KpiData.PredictorData(entry.getKey(),
                  entry.getValue())).collect(Collectors.toList());
        }

        if (!catMap.isEmpty()) {
          kpid.categoricalfeatures = catMap.entrySet().stream().map(entry ->
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

