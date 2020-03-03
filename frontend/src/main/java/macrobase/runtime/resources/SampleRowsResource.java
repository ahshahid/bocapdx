package macrobase.runtime.resources;

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
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Path("/sampleRows")
@Produces(MediaType.APPLICATION_JSON)

public class SampleRowsResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

  @Context
  private HttpServletRequest request;



  static class SampleRowsResponse {
    public List<List<String>> rows = null;
    public long rowCount = 0;
    public String errorMessage = null;
  }

  public SampleRowsResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public SampleRowsResponse getSample(SchemaResource.SchemaRequest schmaReq) {
    SampleRowsResponse response = new SampleRowsResponse();
    HttpSession ss = request.getSession();
    try {
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);
      TableData td = TableManager.getTableData(schmaReq.tablename, ingester);
      response.rows = td.getSampleRows();
      response.rowCount = td.getTotalRowsCount();
    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }


}
