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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/graph")
@Produces(MediaType.APPLICATION_JSON)

public class GraphResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(GraphResource.class);

  private static int DEEP_EXPL = 0;
  private static int FAST_EXPL = 1;
  private static String BAR_CHART = "bar";
  private static String AREA_CHART = "area";
  private static String HISTOGRAM = "histogram";
  private static String CLUSTERED_BAR_CHART = "clustered_bar";

  private static String QUERY_DEEP_METRIC_CONTI = "select count(*), avg(%1$s), %2$s from %3$s group by %2$s";
  private static String QUERY_DEEP_METRIC_CAT = "select count(*), %1$s, %2$s from %3$s group by %1$s, %2$s";
  @Context
  private HttpServletRequest request;

  static class GraphRequest {
    public int workflowid;
    public String metric;  //outlier column
    public String feature;
    public int graphFor;
  }

  static class GraphResponse {

    public String errorMessage;
    public String graphType;
    public List<GraphPoint> dataPoints;
    public boolean isMetricNumeric;
    public boolean isFeatureNumeric;
    public boolean isFeatureRange;
  }

  static class GraphPoint {
    public long numElements;
    public String feature;
    public String metric;

    public String featureLowerBound;
    public String featureUpperBound;

    GraphPoint(long n, String f, String m, String flb,  String fub) {
      this.numElements = n;
      this.feature = f;
      this.metric = m;
      this.featureLowerBound = flb;
      this.featureUpperBound = fub;
    }
  }

  public GraphResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public GraphResponse getGraphData(GraphRequest gr) {
    GraphResponse response = null ;
    HttpSession ss = request.getSession();
    try {
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);

      final TableData tdOrig = TableManager.getTableData(gr.workflowid);
      int actualFeatureColType = tdOrig.getColumnData(gr.feature).sqlType;
      final TableData preppedTableData = tdOrig.getTableDataForFastInsights();
      TableData.ColumnData metricColCd = tdOrig.getColumnData(gr.metric);
      List<Schema.SchemaColumn> actualCols = preppedTableData.getSchema().getColumns().stream().
          filter(sc -> sc.getName().
              equalsIgnoreCase(MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX + gr.metric)).
          collect(Collectors.toList());
      String metricCol = gr.metric;
      boolean metricColShadowExists = !actualCols.isEmpty();
      if (metricColShadowExists) {
        metricCol = actualCols.get(0).getName();
      }
      String tableName = preppedTableData.getTableOrView();
      if (gr.graphFor == DEEP_EXPL) {
        response = this.getResponseForDeepInsights(tableName, gr.feature, actualFeatureColType,
            gr.metric, metricCol, ingester, metricColCd);
      }


    } catch (Exception e) {
      response = new GraphResponse();
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }
    return response;
  }

  private GraphResponse getResponseForDeepInsights(String tableName, String featureCol,int actualFeatureColType,
      String metricCol,
      String unbinnedMetricCol, SQLIngester ingester, TableData.ColumnData metricColCd) throws SQLException {
    if (!metricColCd.skip && metricColCd.ft.equals(FeatureType.continuous)) {
      String query = String.format(QUERY_DEEP_METRIC_CONTI, unbinnedMetricCol, featureCol, tableName);
      ResultSet rs = ingester.executeQuery(query);
      int featureColType = rs.getMetaData().getColumnType(3);
      List<GraphPoint> dataPoints = new LinkedList<>();
      boolean isMetricNumeric = true;
      boolean isFeatureNumeric = false;
      boolean isFeatureRange = false;
      int numRows = 0;
      if (featureColType == Types.VARCHAR) {
        if (actualFeatureColType == Types.VARCHAR) {
          isFeatureNumeric = false;
          isFeatureRange = false;
        } else {
          isFeatureNumeric = true;
          isFeatureRange = true;
        }
      } else {
        isFeatureNumeric = true;
        isFeatureRange = false;
      }
      while (rs.next()) {
        long count = rs.getLong(1);
        double avg = rs.getDouble(2);
        String key = null;
        String lb = null;
        String ub = null;

        if (featureColType == Types.VARCHAR) {
          key = rs.getString(3);
          if (isFeatureNumeric && isFeatureRange) {
            lb = key.substring(0, key.indexOf(" - ")).trim();
            ub = key.substring(key.indexOf(" - ")).trim();
          }
        } else {
          key = rs.getObject(3).toString();
        }

        GraphPoint gp = new GraphPoint(count, key, String.valueOf(avg), lb, ub);
        dataPoints.add(gp);
        ++numRows;
      }

      GraphResponse gr = new GraphResponse();
      gr.isFeatureNumeric = isFeatureNumeric;
      gr.isFeatureRange = isFeatureRange;
      gr.isMetricNumeric = isMetricNumeric;
      gr.dataPoints = dataPoints;
      gr.graphType = numRows > 50? AREA_CHART : isFeatureRange ? HISTOGRAM : BAR_CHART;
      return gr;

    } else if (!metricColCd.skip && metricColCd.ft.equals(FeatureType.categorical)) {
      String query = String.format(QUERY_DEEP_METRIC_CAT,featureCol, metricCol, tableName);
      ResultSet rs = ingester.executeQuery(query);
      int featureColType = rs.getMetaData().getColumnType(2);
      int metricColType = rs.getMetaData().getColumnType(3);
      List<GraphPoint> dataPoints = new LinkedList<>();
      boolean isMetricNumeric = false;
      boolean isFeatureNumeric = false;
      boolean isFeatureRange = false;
      int numRows = 0;
      if (featureColType == Types.VARCHAR) {
        if (actualFeatureColType == Types.VARCHAR) {
          isFeatureNumeric = false;
          isFeatureRange = false;
        } else {
          isFeatureNumeric = true;
          isFeatureRange = true;
        }
      } else {
        isFeatureNumeric = true;
        isFeatureRange = false;
      }

      if (metricColCd.sqlType == Types.VARCHAR) {
        isMetricNumeric = false;
      } else {
        isMetricNumeric = true;
      }

      while (rs.next()) {
        long count = rs.getLong(1);
        String key = null;
        String lb = null;
        String ub = null;

        if (featureColType  == Types.VARCHAR) {
          key = rs.getString(2);
          if (isFeatureNumeric && isFeatureRange) {
            lb = key.substring(0, key.indexOf(" - ")).trim();
            ub = key.substring(key.indexOf(" - ")).trim();
          }
        } else {
          key = rs.getObject(2).toString();
        }

        String kpi = rs.getObject(3).toString();

        GraphPoint gp = new GraphPoint(count, key, kpi, lb, ub);
        dataPoints.add(gp);
        ++numRows;
      }

      GraphResponse gr = new GraphResponse();
      gr.isFeatureNumeric = isFeatureNumeric;
      gr.isFeatureRange = isFeatureRange;
      gr.isMetricNumeric = isMetricNumeric;
      gr.dataPoints = dataPoints;
      gr.graphType = CLUSTERED_BAR_CHART;
      return gr;
    }
    return  null;
  }

  private GraphResponse getResponseForFastInsights() {
    return null;
  }

}

