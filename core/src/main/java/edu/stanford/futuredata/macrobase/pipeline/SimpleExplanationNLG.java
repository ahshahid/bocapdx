package edu.stanford.futuredata.macrobase.pipeline;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.visualization.datasource.datatable.DataTable;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanationResult;
import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

// Just a hack for initial prototype ....

public class SimpleExplanationNLG implements Explanation {

    private final PipelineConfig conf;
    private final String outputTable;
    private final APLExplanation explainObj;
    private final Map<String, String> colDescriptions;
    private final String metric;
    private final Connection conn;
    private final int workflowid;
    private final String originalMetricColumn;


    static class EachExplanation {
        public String explanationStr;
        public List<String> features;
        public List<DataTable> graphs;
        EachExplanation(String expl, List<String> ft, List<DataTable> graphs) {
            this.explanationStr = expl;
            this.features = ft;
            this.graphs = graphs;
        }
    }

    public SimpleExplanationNLG(PipelineConfig conf, APLExplanation explainObj ,
        String outputTable, String metric, Connection conn, int workflowid, String originalMetricColumn ) throws Exception {
        this.conf = conf;
        this.explainObj =explainObj;
        this.outputTable = outputTable;
        this.metric = metric;
        this.conn = conn;
        this.colDescriptions = getColDescriptions(conn);
        this.workflowid = workflowid;
        this.originalMetricColumn = originalMetricColumn;


    }

    @JsonProperty("header")
    public String getNlgHeaderText() {
        try {
            return this.explainAsHeader(false);
        } catch (Exception e) {
            e.printStackTrace();
            return "Error in getting NLG text";
        }
    }

    /*@JsonProperty("rawExplanation")*/
    public Explanation rawExplanation() {
      return this.explainObj;
    }

    @JsonProperty("nlgExplanation")
    public List<EachExplanation> getNlgText() {
      try {
          return rawExplainTable();
      } catch (Exception e) {
          e.printStackTrace();
         return Collections.singletonList(new EachExplanation("Error in getting NLG text", Collections.EMPTY_LIST,
                 Collections.emptyList()));
      }
    }

    private String getFooter() {
        return "\"\\n==========================================================\\n\"";
    }

    private String explainAsHeader(boolean detailedOutput) throws Exception {
        StringBuffer outputText = new StringBuffer();
        if (detailedOutput) {
            outputText.append(this.explainObj.prettyPrint());
        }
        int count = explainObj.getResults().size();
        if (count == 0) {
            outputText.append("Oops. The analysis did not generate any explanations. " +
                "We suggest looking for more rare event patterns and try again.");
            return outputText.toString();
        }
        else if (count > 10) {
            outputText.append("Well, turns out your analysis generated quite a few explanations. " +
                "While our NLG in the future will summarize all these effectively, for now," +
                " we present the most important ones. You could change 'Support' for more common" +
                " patterns and try again. You can view all the explanations here(URL).");
        }

       outputText.append("\n\nYour OBJECTIVE :: " + conf.get("objective", "Not specified") );
        outputText.append("\nAnalysis for your objective meant we had to carry out statistical analysis and " +
            "comparison of records where \"" + getPredicateString() + "\" with the rest. \n");

        outputText.append("Good news. The FastInsights engine produced " + count + " specific facts that " +
            "drive your objective. Here are the important ones(upto 10)  ...\n");

        return outputText.toString();
    }



    public String prettyPrint() {

       try {
           StringBuilder outputText = new StringBuilder();
           outputText.append(explainAsHeader(true));
           List<EachExplanation> explanations = rawExplainTable();
           explanations.forEach(expl -> outputText.append(expl.explanationStr));
           outputText.append(getFooter());
           return outputText.toString();
       } catch (Exception e) {
           e.printStackTrace();
           return this.explainObj.prettyPrint();
       }
    }

    public double numTotal() {
       return this.explainObj.numTotal();
    }

    private List<EachExplanation> rawExplainTable() throws Exception {
        List rows = getRows("select * from " + outputTable + " order by global_ratio desc, support limit 10",
            this.conn).getRows();
        List<EachExplanation> explanations = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            explanations.add(rawExplainRow((RowSet.Row)(rows.get(i)), i));
        }
        return explanations;
    }


    public EachExplanation rawExplainRow(RowSet.Row r,  int rowNum) throws Exception {
        String supportString ="";
        String ratioString = "";
        long supportPercent = 0;
        final TableData td ;
        if (workflowid != -1) {
            td = TableManager.getTableData(workflowid);
        } else {
            td = null;
        }
        StringBuilder outputText = new StringBuilder();
        List<String> features = new ArrayList<>();
        outputText.append("("  + (++rowNum) + ")" + " When the value of ");
        List<ColumnValue> l = r.getColumnValues();
        String temp = "";
        for (int i = 0; i < l.size() ; i++) {
            String c = l.get(i).getColumn().toLowerCase();
            String value = l.get(i).getValue();
            if (c.equals("global_ratio")) ratioString = getRatioString(value);
            else if (c.equals("support")) {
                supportString = getNewSupportString(value);
                supportPercent = Math.round(Double.parseDouble(value) * 100);
            }

            // Skip non domain attributes ...
            if (c.equals("global_ratio") || c.equals("outliers") || c.equals("count") || c.equals("support"))
                continue;
            if (value == null || value.equalsIgnoreCase("NULL")) continue;
            String actualColumn = l.get(i).getColumn();
            features.add(actualColumn);
            temp += (getDescription(actualColumn) + " (" + actualColumn + ")" +" is " + value);
            if (i < (l.size() -1) ) temp += " and ";
        }
        if (temp.endsWith("and ")) outputText.append(temp.substring(0, temp.lastIndexOf("and"))) ;

        if (isOutcomeBinary()) {
            outputText.append(", your metric " + metric + " is " +
                    ratioString + "times higher than usual. This increased " + supportString);
        }
        else {
            outputText.append(", the chance (or risk) of meeting your objective is " +
                    ratioString + " times higher than usual. This represents " +
                    supportPercent + " percent of all records that meet your objective.");
        }

        List<DataTable> graphs = Collections.emptyList();
        if (workflowid != -1) {
            graphs = features.stream().map(
                    ft -> td.getDeepInsightGraphData(this.originalMetricColumn, ft, conn)).
                    collect(Collectors.toList());
        }
        return  new EachExplanation(outputText.toString(), features, graphs);
    }

    private String getDescription(String column) {
        String out;
        if ((out = colDescriptions.get(column)) == null)
            if((out = colDescriptions.get(column.toUpperCase())) == null)
                if ((out = colDescriptions.get(column.toLowerCase())) == null)
                    return column;
        return out;
    }

    private Map<String,String> getColDescriptions(Connection conn) throws Exception{
        String table = BasicBatchPipeline.removeSuffix(outputTable, "_prepped_Explained").toLowerCase();
       // String sql = "Select columnname, description from columnDescriptions where tablename = '" + table + "'";
        String sql = "Select columnname, description from columnDescriptions" ;
        RowSet rs = getRows(sql, conn);
        Map<String, String> m = new HashMap<>();
        for (RowSet.Row r: rs.getRows()) {
            List<ColumnValue> v = r.getColumnValues();
            String key = v.get(0).getValue();
            String val = v.get(1).getValue();
            m.put(key, val);
        }
        return m;
    }

    private String getRatioString(String v) {
        double d = Double.parseDouble(v);
        if (d >= 1.5 && d < 1.7) return " 1.5 to 1.7 ";
        if (d >= 1.7 && d < 1.9) return " 1.7 to 1.9 ";
        if (d >= 1.9 && d < 2.0) return " 1.9 to 2.0 ";
        if (d >= 2.0 && d < 3.0) return " two ";
        if (d >= 3.0 && d < 4.0) return " three ";
        if (d >= 4.0 && d < 5.0) return " four ";
        if (d >= 5.0 && d < 6.0) return " five ";
        if (d >= 6.0 && d < 7.0) return " six ";
        if (d >= 7.0 && d < 8.0) return " seven ";

        else return " eight+ ";
    }

    private String getSupportString(String v) {
        double d = Double.parseDouble(v);
        if (d < .1 && d > 0.01) return  " rare. ";
        else if (d < 0.01 ) return  " very rare. ";
        else if (d >= .1 && d < .5) return " fairly common.";
        else return " very common. ";
    }

    // If the outcome variable is binary, we are sure that outcome improves by Support %?
    private String getNewSupportString(String v) {
        //return " metric by x% "
        long i  = Math.round(Double.parseDouble(v) * 100);
        return metric + " by " + i + " percent.";
    }

    private String getPredicateString() throws Exception{
        String s = "";
        if (conf.get("classifier").equals("predicate")) {
            s += conf.get("metric", "unknown").trim();
            String relation = conf.get("predicate", "unknown").trim();
            switch (relation) {
                case "==":
                    s += " is "; break;
                case ">":
                    s += " is greater than "; break;
                case "<":
                    s += " is less than "; break;
                case "!=":
                    s += " is not equal to "; break;
                case ">=":
                    s += " is greater than or equal to "; break;
                case "<=":
                    s += " is less than or equal to "; break;
                case "unknown":
                    s += " unknown "; break;
            }
            Object rawCutoff = conf.get("cutoff");
            boolean isStrPredicate = rawCutoff instanceof String;
            if (isStrPredicate) {
                String strCutoff = ((String) rawCutoff).toLowerCase();
                if (strCutoff.equals("yes") || strCutoff.equals("true") || strCutoff.equals("1"))
                    s += " true ";
                else
                    s += " " + rawCutoff + " ";
            } else {
                s += " " + rawCutoff + " ";
            }
        } else {
            //TODO: Fix support for percentile ...
            s += conf.get("cutoff") + " percentile ";
        }

        return s;
    }

    // Hack ... just use Conf to figure out if Outcome is binary .. fix later.
    private boolean isOutcomeBinary() {
        Object rawCutoff = conf.get("cutoff");
        boolean isStrPredicate = rawCutoff instanceof String;
        if (isStrPredicate) {
            String strCutoff = ((String) rawCutoff).toLowerCase();
            if (strCutoff.equals("yes") || strCutoff.equals("true") || strCutoff.equals("1"))
                return true;
        }
        return false;
    }


    private static RowSet getRows(String query, Connection connection) throws SQLException {

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        List<RowSet.Row> rows = Lists.newArrayList();
        while (rs.next()) {
            List<ColumnValue> columnValues = Lists.newArrayList();

            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                columnValues.add(
                        new ColumnValue(rs.getMetaData().getColumnName(i),
                                rs.getString(i)));
            }
            rows.add(new RowSet.Row(columnValues));
        }

        return new RowSet(rows);
    }



}
