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
    public List<String> depcols;
    public List<Double> corrs;
    public String errorMessage;
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
      DependencyData[] dd = new DependencyData[fir.kpicols.size()];
      int index = 0;
      for(String kpiCol: fir.kpicols) {
        dd[index] = td.getDependencyData(kpiCol, ingester);
      }

    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }
}
