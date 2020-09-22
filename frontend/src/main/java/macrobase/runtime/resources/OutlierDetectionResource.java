package macrobase.runtime.resources;

import io.boca.internal.tables.FeatureType;
import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;
import io.boca.internal.tables.Utils;
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
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Path("/detectOutliers")
@Produces(MediaType.APPLICATION_JSON)

public class OutlierDetectionResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(OutlierDetectionResource.class);

  @Context
  private HttpServletRequest request;

  static class OutlierDetectionRequest {
    public int workflowid;
    public List<String> kpicols;
    public String outlierpredicate;
    public String fence;

  }


  static class OutlierDetectionResponse {
    public List<List<String>> rows = null;
    public String errorMessage = null;
  }

  public OutlierDetectionResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public OutlierDetectionResponse getOutliers(OutlierDetectionRequest req) {
    OutlierDetectionResponse response = new OutlierDetectionResponse();
    HttpSession ss = request.getSession();

    try {
      TableData td = TableManager.getTableData(req.workflowid);
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);
      if (req.outlierpredicate != null && !req.outlierpredicate.trim().isEmpty()) {
        String query = "select * from " + td.getTableOrView() + " where " + req.outlierpredicate;
        ResultSet rs = ingester.executeQuery(query);
        response.rows = Utils.convertResultsetToRows(rs);
      } else {
        //Get kpi cols
        List<TableData.ColumnData> continousCols = req.kpicols.stream().map(col -> td.getColumnData(col)).
            filter(cd -> cd.ft != null && cd.ft.equals(FeatureType.continuous)).collect(Collectors.toList());
        
      }

    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }


}
