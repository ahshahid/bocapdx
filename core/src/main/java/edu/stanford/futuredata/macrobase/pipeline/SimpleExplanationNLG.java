package edu.stanford.futuredata.macrobase.pipeline;

import com.google.common.collect.Lists;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

// Just a hack for initial prototype ....

public class SimpleExplanationNLG {

    private final PipelineConfig conf;
    private final String outputTable;
    private final APLExplanation explainObj;

    public SimpleExplanationNLG(PipelineConfig conf, APLExplanation explainObj , String outputTable ) throws Exception {
        this.conf = conf;
        this.explainObj =explainObj;
        this.outputTable = outputTable;
    }

    public String explainAsText() throws Exception {
        StringBuffer outputText = new StringBuffer();
        int count = explainObj.getResults().size();
        if (count == 0) {
            outputText.append("\nOops. The analysis did not generate any explanations. " +
                    "We suggest looking for more rare event patterns and try again.");
        }
        else if (count > 10) {
            outputText.append("Well, turns out your analysis generated quite a few explanations. " +
                    "While our NLG in the future will summarize all these effectively, for now," +
                    " we present the most important ones. You could change 'Support' for more common" +
                    " patterns and try again. You can view all the explanations here(URL).");
        }

        outputText.append("\n Your Objective :: " + conf.get("objective", "Not specified") +
                              "\n==========================================================\n");

        outputText.append("Great news. The FastInsights engine produced " + count + " specific facts that " +
                "drive your objective. Here they are ordered on importance ...\n");
        rawExplainTable(outputText);
        outputText.append("\n==========================================================\n");
        return outputText.toString();
    }


    public void rawExplainTable(StringBuffer outputText) throws Exception {
        List rows = getRows("select * from " + outputTable + " order by global_ratio desc, support limit 10").getRows();
        for (int i = 0; i < rows.size(); i++) {
            rawExplainRow((RowSet.Row)(rows.get(i)), outputText, i);
        }
    }


    public void rawExplainRow(RowSet.Row r, StringBuffer outputText, int rowNum) throws Exception {
        String supportString ="";
        String ratioString = "";

        outputText.append("\n("  + (++rowNum) + ")" + " When the value of ");
        List<ColumnValue> l = r.getColumnValues();
        String temp = "";
        for (int i = 0; i < l.size() ; i++) {
            String c = l.get(i).getColumn().toLowerCase();
            String value = l.get(i).getValue();
            if (c.equals("global_ratio")) ratioString = getRatioString(value);
            else if (c.equals("support")) supportString = getSupportString(value);

            // Skip non domain attributes ...
            if (c.equals("global_ratio") || c.equals("outliers") || c.equals("count") || c.equals("support"))
                continue;
            if (value == null || value.equalsIgnoreCase("NULL")) continue;

            temp += (l.get(i).getColumn() + " is " + value);
            if (i < (l.size() -1) ) temp += " and ";
        }
        if (temp.endsWith("and ")) outputText.append(temp.substring(0, temp.lastIndexOf("and"))) ;

        outputText.append(", the risk or chance of " + getPredicateString() + " is " +
                ratioString + "times higher. This fact pattern is" + supportString ) ;
    }

    private String getRatioString(String v) {
        double d = Double.parseDouble(v);
        if (d > 1.5 && d < 1.7) return " 1.5 to 1.7 ";
        if (d > 1.7 && d < 1.9) return " 1.7 to 1.9 ";
        if (d > 2.0 && d < 3.0) return " two ";
        if (d > 3.0 && d < 4.0) return " three ";
        if (d > 4.0 && d < 5.0) return " four ";
        if (d > 5.0 && d < 6.0) return " five ";
        if (d > 6.0 && d < 7.0) return " six ";
        if (d > 7.0 && d < 8.0) return " seven ";

        else return " eight+ ";
    }

    private String getSupportString(String v) {
        double d = Double.parseDouble(v);
        if (d < .1 && d > 0.01) return  " rare. ";
        else if (d < 0.01 ) return  " very rare. ";
        else if (d >= .1 && d < .5) return " fairly common.";
        else return " very common. ";
    }

    private String getPredicateString() throws Exception{
        String s = "";
        if (conf.get("classifier").equals("predicate")) {
            s += conf.get("metric", "unknown").trim();
            String relation = conf.get("predicate", "unknown").trim();
            switch (relation) {
                case "==":
                    s += " being "; break;
                case ">":
                    s += " greater than "; break;
                case "<":
                    s += " less than "; break;
                case "!=":
                    s += " not equal to "; break;
                case ">=":
                    s += " greater than or equal to "; break;
                case "<=":
                    s += " less than or equal to "; break;
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
        } else
            throw new Exception("Please help add support for non-predicate classifier in SimpleNLG ...");

        return s;
    }

    private RowSet getRows(String query) throws SQLException {
        Connection connection = getConnection();

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

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(conf.get("inputURI"));
    }

}
