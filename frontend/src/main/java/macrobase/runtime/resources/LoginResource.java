package macrobase.runtime.resources;

import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
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

@Path("/login")
@Produces(MediaType.APPLICATION_JSON)
public class LoginResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(LoginResource.class);

  @Context
  private HttpServletRequest request;

  static class LoginRequest {
    public String username;
    public String password;
  }
  static class LoginResponse {
    public List<String> results;
    public String errorMessage;
  }



  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public LoginResource.LoginResponse login(LoginRequest req) {
    LoginResource.LoginResponse response = new LoginResponse();
    HttpSession ss = request.getSession();
    ss.setAttribute(MacroBaseConf.DB_USER, req.username);
    ss.setAttribute(MacroBaseConf.DB_PASSWORD, req.password);
    try {
      MacroBaseConf confNew = conf.copy();
      confNew.set(MacroBaseConf.DB_USER, req.username);
      confNew.set(MacroBaseConf.DB_PASSWORD, req.password);
      SQLIngester ingester = (SQLIngester) getLoader(confNew);
      ss.setAttribute(MacroBaseConf.SESSION_INGESTER, ingester);
      response.results = ingester.getTables();

    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }


  public LoginResource(MacroBaseConf conf) {
    super(conf);
  }

}
