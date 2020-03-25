package macrobase.runtime.resources;

import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.sql.Types;
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
    public String metric;  //outlier column
    public String objective;
    public Map<String, Object> optionalConf;
  }

  static class DeepInsightResponse {

    public String errorMessage;
    public Explanation expl = null;
    private DeepInsightResponse() {}
    private DeepInsightResponse (Explanation expl) {
      this.expl = expl;
    }

  }


  public DeepInsightResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public DeepInsightResponse getDeepInsight(DeepInsightRequest dir) {
    DeepInsightResponse response ;
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
      // validate optionalConf
      Map<String, Object> optionalConf = dir.optionalConf;
      if (optionalConf != null) {
        // validate keys
        optionalConf.keySet().stream().forEach(key -> {
          boolean found = false;
          for(String param: MacroBaseConf.optionalParams) {
            if (key.equals(param)) {
              found = true;
              break;
            }
          }
          if (!found) {
            throw new RuntimeException("Unknown optional param = " + key);
          }
        });
      } else {
        optionalConf = new HashMap<>();
      }
      Object pred = optionalConf.get(MacroBaseConf.PRED_KEY);
      if (pred != null) {
        optionalConf.put(MacroBaseConf.CLASSIFIER_KEY, MacroBaseConf.CLASSIFIER_PRED);
      } else {
        assert tdOrig.getColumnData(dir.metric).sqlType == Types.DOUBLE;
        optionalConf.put(MacroBaseConf.CLASSIFIER_KEY, MacroBaseConf.CLASSIFIER_PERCENTILE);
      }


      optionalConf.put(MacroBaseConf.BASE_TABLE_KEY, tableName);
      optionalConf.put(MacroBaseConf.METRIC_KEY, metricCol);
      optionalConf.put(MacroBaseConf.PROVIDED_CONN_KEY, ingester.getConnection());
      optionalConf.put(MacroBaseConf.SUMMARIZER_KEY, "apriori");
      /*
      if (optionalConf.containsKey(MacroBaseConf.EXTRA_PRED_KEY)) {
        String extraPred = (String)optionalConf.get(MacroBaseConf.EXTRA_PRED_KEY);
        if (extraPred != null && !extraPred.trim().isEmpty()) {
          String query = "select * from " + tableName + " " + extraPred;
          Select select = (Select)CCJSqlParserUtil.parse(query);
          Expression expr = ((PlainSelect)select.getSelectBody()).getWhere();
          expr.accept(new ExpressionVisitorAdapter(){
            @Override
            public void visit(Column column) {
              final String columnName = column.getColumnName();
              List<Schema.SchemaColumn> actualCols = preppedTableData.getSchema().getColumns().stream().
                  filter(sc -> sc.getName().
                      equalsIgnoreCase(MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX + columnName)).
                  collect(Collectors.toList());
              String newColumnName = columnName;
              if (!actualCols.isEmpty()) {
                newColumnName = actualCols.get(0).getName();
              }
              column.setColumnName(newColumnName);
            }
          });


        }
      }

      */
      PipelineConfig config = new PipelineConfig(optionalConf);
      BasicBatchPipeline bbp = new BasicBatchPipeline(config);
      Explanation expl = bbp.results();
      response = new DeepInsightResponse(expl);

    } catch (Exception e) {
      response = new DeepInsightResponse();
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }
}

