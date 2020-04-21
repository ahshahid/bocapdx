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
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/deepInsight")
@Produces(MediaType.APPLICATION_JSON)

public class DeepInsightResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(DeepInsightResource.class);

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
      TableData.ColumnData metricColCd = tdOrig.getColumnData(dir.metric);
      List<Schema.SchemaColumn> actualCols = preppedTableData.getSchema().getColumns().stream().
          filter(sc -> sc.getName().
              equalsIgnoreCase(MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX + dir.metric)).
          collect(Collectors.toList());
      String metricCol = dir.metric.toLowerCase();
      boolean metricColShadowExists = !actualCols.isEmpty();
      if (metricColShadowExists) {
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
      optionalConf.put(MacroBaseConf.OBJECTIVE_KEY, dir.objective);
      String pred = (String)optionalConf.get(MacroBaseConf.PRED_KEY);
      if (pred != null) {
        optionalConf.put(MacroBaseConf.CLASSIFIER_KEY, MacroBaseConf.CLASSIFIER_PRED);

        // parse the pred

        String query = "select * from " + tableName + " where " + pred;
        Object[] results = parseExpression(query);

        Object cutoff = results[2];
        Object newCutOff = cutoff;
        if (metricColShadowExists) {
          // this means that actual metric column in prepped table is of data type double, irrespective of type in
          // actual table
          if (cutoff != null) {
            newCutOff = convertToDouble(cutoff);
          }
        } else {
          // cateoriocal column, all to be treated as string
          if (!(cutoff instanceof String)) {
            if (cutoff instanceof  Integer) {
              newCutOff = String.valueOf((Integer)((Integer)cutoff).intValue());
            } else if (cutoff instanceof  Long) {
              newCutOff = String.valueOf((Long)((Long)cutoff).longValue());
            }
            else if (cutoff instanceof  Float) {
              newCutOff = String.valueOf((Float)((Float)cutoff).floatValue());
            } else if (cutoff instanceof  Double) {
              newCutOff = String.valueOf((Double)((Double)cutoff).doubleValue());
            }
          }
        }
        optionalConf.put(MacroBaseConf.PRED_KEY, results[1]);
        optionalConf.put(MacroBaseConf.CUT_OFF_KEY, newCutOff);

      } else {
        assert metricColCd.sqlType == Types.DOUBLE;
        optionalConf.put(MacroBaseConf.CLASSIFIER_KEY, MacroBaseConf.CLASSIFIER_PERCENTILE);
      }
      if (!optionalConf.containsKey(MacroBaseConf.MAX_ORDER_KEY)) {
        optionalConf.put(MacroBaseConf.MAX_ORDER_KEY, 2);
      }
      if (optionalConf.containsKey(MacroBaseConf.MIN_SUPPORT_KEY)) {
        Object val = optionalConf.get(MacroBaseConf.MIN_SUPPORT_KEY);
        val = val != null ?convertToDouble(val) : val;
        optionalConf.put(MacroBaseConf.MIN_SUPPORT_KEY, val);
        optionalConf.put(MacroBaseConf.MIN_SUPPORT, val);
      }

      if (optionalConf.containsKey(MacroBaseConf.MIN_RATIO_KEY)) {
        Object val = optionalConf.get(MacroBaseConf.MIN_RATIO_KEY);
        val = val != null ?convertToDouble(val) : val;
        optionalConf.put(MacroBaseConf.MIN_RATIO_KEY, val);
        optionalConf.put(MacroBaseConf.MIN_OI_RATIO, val);
      }

      List<String> attributes = (List<String>)optionalConf.getOrDefault(
          MacroBaseConf.ATTRIBUTES_KEY, Collections.emptyList());
      if (attributes.isEmpty()) {
       List<String> allProj =  tdOrig.getSchema().getColumns().stream().
            filter(sc -> !sc.getName().equalsIgnoreCase(dir.metric)).map(sc -> sc.getName()).
            collect(Collectors.toList());
       optionalConf.put(MacroBaseConf.ATTRIBUTES_KEY, allProj);
      } else {
        optionalConf.put(MacroBaseConf.ATTRIBUTES_KEY,attributes.stream().
                map(x -> x.toLowerCase()).collect(Collectors.toList()));
      }

      optionalConf.put(MacroBaseConf.BASE_TABLE_KEY, tableName);
      optionalConf.put(MacroBaseConf.METRIC_KEY, metricCol);
      optionalConf.put(MacroBaseConf.PROVIDED_CONN_KEY, ingester.getConnection());
      optionalConf.put(MacroBaseConf.SUMMARIZER_KEY, "apriori");
      optionalConf.put(MacroBaseConf.WORKFLOWID_KEY, dir.workflowid);
      optionalConf.put(MacroBaseConf.ORIGINAL_METRIC_COL_KEY, dir.metric.toLowerCase());
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

  // returns 3 element array. the first two elements are strings ( column name & operator)
  // the third is the value  ( string, int, double, null any thing)
  // for now ignore the 1st column which will always have to be kpi
  private Object[] parseExpression(String query) throws Exception {

    Select select = (Select)CCJSqlParserUtil.parse(query);
    Expression expr = ((PlainSelect)select.getSelectBody()).getWhere();
    final Object[] results = new Object[3];
    expr.accept(new ExpressionVisitorAdapter(){
      protected void visitBinaryExpression(BinaryExpression expr) {
        results[1] = expr.getStringExpression();
        expr.getLeftExpression().accept(this);
        expr.getRightExpression().accept(this);
      }
      public void visit(NullValue value) {
        results[2] = null;
      }

      public void visit(DoubleValue value) {
        results[2] = value.getValue();
      }

      public void visit(LongValue value) {
        results[2] = value.getValue();
      }

      public void visit(DateValue value) {
        results[2] = value.getValue();
      }

      public void visit(TimeValue value) {
        results[2] = value.getValue();
      }

      public void visit(TimestampValue value) {
        results[2] = value.getValue();
      }


      public void visit(StringValue value) {
        results[2] = value.getValue();
      }

      public void visit(Column column) {
        results[0] = column.getColumnName().toLowerCase();
      }

      public void visit(IsNullExpression expr) {
        results[1] = expr.isNot() ? "IS NOT " : "IS";
        results[2] = null;
        expr.getLeftExpression().accept(this);
      }

    });

    return results;

  }

  private static double convertToDouble(Object val) {
    if (val instanceof  String) {
      try {
        return  Double.valueOf(Integer.parseInt((String)val));
      } catch (Exception e){
        //ignore
      }
      try {
        return  Double.valueOf(Long.parseLong((String)val));
      } catch (Exception e){
        //ignore
      }
      try {
        return  Double.valueOf(Float.parseFloat((String)val));
      } catch (Exception e){
        //ignore
      }
      try {
        return  Double.parseDouble((String)val);
      } catch (Exception e){
        //ignore
      }
      throw new RuntimeException("unable to convert " + val +" to double");
    } else  {
      if (val instanceof  Integer) {
        return  ((Integer)val).doubleValue();
      } else if (val instanceof  Long) {
        return ((Long)val).doubleValue();
      }
      else if (val instanceof  Float) {
       return ((Float)val).doubleValue();
      } else if (val instanceof  Double) {
        return ((Double)val).doubleValue();
      }
      throw new RuntimeException("unable to convert " + val +" to double");
    }
  }
}

