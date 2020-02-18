package macrobase.runtime.resources;

import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import macrobase.MacroBase;
import macrobase.analysis.pipeline.BasicBatchedPipeline;
import macrobase.analysis.pipeline.Pipeline;
import macrobase.analysis.result.AnalysisResult;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.SQLIngester;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("//sampleRows")
@Produces(MediaType.APPLICATION_JSON)
public class RefreshTablesResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(LoginResource.class);

  @Context
  private HttpServletRequest request;

 @GET
  @Consumes(MediaType.APPLICATION_JSON)
  public LoginResource.LoginResponse refresh() {
    LoginResource.LoginResponse response = new LoginResource.LoginResponse();
    HttpSession ss = request.getSession();
    try {
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);
      response.results = ingester.getTables();

    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }


  public RefreshTablesResource(MacroBaseConf conf) {
    super(conf);
  }

}
