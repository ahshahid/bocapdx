package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.*;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLCountMeanShiftSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLOutlierSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.FPGrowthSummarizer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Simplest default pipeline: load, classify, and then explain
 * Only supports operating over a single metric
 */
public class BasicBatchPipeline implements Pipeline {
    private final PipelineConfig conf;
    Logger log = LoggerFactory.getLogger(Pipeline.class);

    private String inputURI = null;
    private String baseTable;
    private String extraPredicate;

    private String classifierType;
    private String metric;
    private double cutoff;
    private Optional<String> meanColumn;
    private String strCutoff;
    private boolean isStrPredicate;
    private boolean pctileHigh;
    private boolean pctileLow;
    private String predicateStr;
    private int numThreads;
    private int bitmapRatioThreshold;

    private String summarizerType;
    private List<String> attributes;
    private String ratioMetric;
    private double minSupport;
    private double minRiskRatio;
    private double meanShiftRatio;
    private int maxOrder;

    private boolean useFDs;
    private int[] functionalDependencies;
    private Connection providedConn;
    private final int workflowid;
    private final String originalMetricCol;


    public BasicBatchPipeline (PipelineConfig conf) {
        this.conf = conf;
        inputURI = conf.get("inputURI");
        this.providedConn = conf.get("providedConnection");
        baseTable = conf.get("baseTable", "NULL");
        extraPredicate = conf.get("extraPredicate", "");
        classifierType = conf.get("classifier", "percentile");
        metric = ((String)conf.get("metric")).toLowerCase();
        workflowid = (Integer)conf.get("workflowid", -1).intValue();
        if (classifierType.equals("predicate") || classifierType.equals("countmeanshift")){
            Object rawCutoff = conf.get("cutoff");
            isStrPredicate = rawCutoff instanceof String;
            if (isStrPredicate) {
                strCutoff = (String) rawCutoff;
            } else {
                cutoff = (double) rawCutoff;
            }
        } else {
            isStrPredicate = false;
            cutoff = conf.get("cutoff", 1.0);
        }

        pctileHigh = conf.get("includeHi",true);
        pctileLow = conf.get("includeLo", true);
        predicateStr = conf.get("predicate", "==").trim();

        summarizerType = conf.get("summarizer", "apriori");
        attributes = conf.get("attributes");
        ratioMetric = conf.get("ratioMetric", "globalRatio");
        minRiskRatio = conf.get("minRatioMetric", 3.0);
        minSupport = conf.get("minSupport", 0.01);
        maxOrder = conf.get("maxOrder", 3);
        numThreads = conf.get("numThreads", Runtime.getRuntime().availableProcessors());
        bitmapRatioThreshold = conf.get("bitmapRatioThreshold", 256);
        this.originalMetricCol = conf.get("originalMetricColumn", metric);

        //if FDs are behind used, parse them into bitmaps. For now, all FDs must be in the first 31 attributes
        useFDs = conf.get("useFDs", false);
        if (useFDs) {
            ArrayList<ArrayList<Integer>> rawDependencies = conf.get("functionalDependencies");
            functionalDependencies = new int[attributes.size()];
            for (ArrayList<Integer> dependency : rawDependencies) {
                for (int i : dependency) {
                    for (int j : dependency) {
                        if (i != j) functionalDependencies[i] |= (1 << j);
                    }
                }
            }
        }
        meanColumn = Optional.ofNullable(conf.get("meanColumn"));
        meanShiftRatio = conf.get("meanShiftRatio", 1.0);
    }

    public Classifier getClassifier() throws MacroBaseException {
        switch (classifierType.toLowerCase()) {
            case "percentile": {
                PercentileClassifier classifier = new PercentileClassifier(metric);
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(pctileHigh);
                classifier.setIncludeLow(pctileLow);
                return classifier;
            }
            case "countmeanshift": {
                if (isStrPredicate) {
                    return new CountMeanShiftClassifier(
                            metric,
                            meanColumn.orElseThrow(
                                    () -> new MacroBaseException("mean column not present in config")), predicateStr,
                            strCutoff);
                } else {
                    return new CountMeanShiftClassifier(
                            metric,
                            meanColumn.orElseThrow(
                                    () -> new MacroBaseException("mean column not present in config")), predicateStr,
                            cutoff);
                }
            }
            case "predicate": {
                if (isStrPredicate){
                    PredicateClassifier classifier = new PredicateClassifier(metric, predicateStr, strCutoff);
                    return classifier;
                }
                PredicateClassifier classifier = new PredicateClassifier(metric, predicateStr, cutoff);
                return classifier;
            }
            default : {
                throw new MacroBaseException("Bad Classifier Type");
            }
        }
    }

    public BatchSummarizer getSummarizer(String outlierColumnName) throws MacroBaseException {
        switch (summarizerType.toLowerCase()) {
            case "fpgrowth": {
                FPGrowthSummarizer summarizer = new FPGrowthSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRiskRatio(minRiskRatio);
                summarizer.setUseAttributeCombinations(true);
                summarizer.setMaxOrder(maxOrder);
                return summarizer;
            }
            case "aplinear":
            case "apriori": {
                APLOutlierSummarizer summarizer = new APLOutlierSummarizer(true);
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRatioMetric(minRiskRatio);
                summarizer.setBitmapRatioThreshold(bitmapRatioThreshold);
                summarizer.setNumThreads(numThreads);
                summarizer.setFDUsage(useFDs);
                summarizer.setFDValues(functionalDependencies);
                summarizer.setMaxOrder(maxOrder);
                return summarizer;
            }
            case "countmeanshift": {
                APLCountMeanShiftSummarizer summarizer = new APLCountMeanShiftSummarizer();
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinMeanShift(meanShiftRatio);
                summarizer.setNumThreads(numThreads);
                summarizer.setMaxOrder(maxOrder);
                return summarizer;
            }
            default: {
                throw new MacroBaseException("Bad Summarizer Type");
            }
        }
    }

    public DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        if (isStrPredicate) {
            colTypes.put(metric, Schema.ColType.STRING);
        }
        else{
            colTypes.put(metric, Schema.ColType.DOUBLE);
        }
        List<String> requiredColumns = new ArrayList<>(attributes);
        if (meanColumn.isPresent()) {
            colTypes.put(meanColumn.get(), Schema.ColType.DOUBLE);
            requiredColumns.add(meanColumn.get());

        }
        requiredColumns.add(metric);
        return PipelineUtils.loadDataFrame(inputURI, colTypes, requiredColumns, baseTable, extraPredicate,
            this.providedConn);
    }

    @Override
    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
        DataFrame df = loadData();
        long elapsed = System.currentTimeMillis() - startTime;

        log.info("Loading time: {} ms", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Metric: {}", metric);
        log.info("Attributes: {}", attributes);

        Classifier classifier = getClassifier();
        classifier.process(df);
        df = classifier.getResults();

        BatchSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());

        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {} ms", elapsed);
        Explanation output = summarizer.getResults();

        //TODO : Hack ... this savetoDB needs to be somewhere else ...
        Connection conn = getConnection();
        if (conn != null){
            String outputTable = baseTable + "_Explained";
            saveDataFrameToJDBC(conn, outputTable,
                getExplanationAsDataFrame(output), true);
            SimpleExplanationNLG e = new SimpleExplanationNLG(conf, (APLExplanation)output, outputTable,
                metric, this.getConnection(), workflowid, this.originalMetricCol);
            System.out.println(e.prettyPrint());
            return e;
        } else {
            return output;
        }
    }



     //private Connection connection = null;

    private Connection getConnection() throws SQLException {
        //TODO: Not thread safe
        if (this.providedConn == null && this.inputURI.contains("jdbc") ) {
            try {
                Class.forName("io.snappydata.jdbc.ClientDriver"); // fix later
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(-1);
            }

            this.providedConn = DriverManager.getConnection(inputURI);
        }
        return this.providedConn;
    }

    public DataFrame getExplanationAsDataFrame(Explanation e) throws Exception{
        if (e instanceof APLExplanation) {
            APLExplanation exp = (APLExplanation)e;
            return ((APLExplanation) e).toDataFrame(attributes);
        }
        else
            throw new Exception("Explanation to DataFrame not supported for this method") ;
    }

    public void saveDataFrameToJDBC(Connection c, String tableName, DataFrame df, boolean replace) throws SQLException {
        Statement s = c.createStatement();
        List<String> cols = df.getSchema().getColumnNames();
        List<String> l = new ArrayList<String>();
        for (int i = 0; i < cols.size(); i++) {
            l.add(stripSuffix(cols.get(i)) + " " + df.getSchema().getColumnTypeByName(cols.get(i))) ;
        }
        System.out.println("=========\n Explanation results stored in --> " + tableName);
        s.execute("Drop table if exists " + tableName); // Avoid this later ... lot more expensive in Gem layer
        String createTableSt = "create table " + tableName + " (" + String.join(",", l) +") using column ";
        System.out.println("CREATING TABLE --> " + createTableSt);
        s.execute(createTableSt);
        if (replace)  // required later when we avoid the drop table.
            s.execute("truncate table " + tableName);

        String prepareStr = "?";
        for (int i = 1; i < cols.size() ; i++) {
            prepareStr = prepareStr + ", ?" ;
        }

        String insertStr = "insert into " + tableName + " values (" + prepareStr + ")";
        System.out.println("Using Insert statement --> " + insertStr + "\n==========");
        PreparedStatement p = c.prepareStatement(insertStr);
        Iterator<Row> j = df.getRowIterator().iterator();
        while (j.hasNext()) {
            Row r = j.next();
            List v = r.getVals();
            for (int k = 0; k < v.size(); k++) {
                p.setObject(k+1, v.get(k));
            }
            p.execute();
        }
        p.close();
        s.close();
    }

    private String stripSuffix(String s) {
        String s1 = removeSuffix(s, "_d_bin");
        return removeSuffix(s1, "_bin");
    }

    public static String removeSuffix(final String s, final String suffix)
    {
        if (s != null && suffix != null && s.endsWith(suffix)){
            return s.substring(0, s.length() - suffix.length());
        }
        return s;
    }

    //Placeholder ... move method to appropriate package/class

    //TODO:  TEST THE FOLLOWING ....
    /****
     *    Find the cardinalities for each column in a table and store into row table. All counts are approx
     */
    String storeDistinctCount = "exec scala" +
            "import org.apache.spark.sql.functions.lit " +
            "val tabname = \"%1\"" +
            "val df = snappysession.table(tabname)\n" +
            "val colList = for (c <- df.columns) yield { s\"approx_count_distinct($c) as $c\"}\n" +
            "val df1 = df.selectExpr( colList: _*)\n" +
            "val df2 = df1.toJSON.withColumn(\"tablename\",lit(tabname))\n" +
            "snappysession.sql(\"create table if not exists columnDistinctCount (tablename varchar(300) primary key, value string)\")\n" +
            "snappysession.put(\"columnDistinctCount\", df2.selectExpr(\"tablename\", \"value\").collect: _*)\n" +
            ";" ;
    public  void storeApproxCountDistinct(String tablename) throws Exception {
        String sql = String.format(storeDistinctCount, tablename);
        getConnection().createStatement().executeQuery(sql);
    }

    //Get approx distinct count for specific Table, columns
    String getApproxCountDistinct =
            "exec scala options(returnDF 'df1')\n" +
            "   import org.apache.spark.sql.functions._\n" +
            "   val table = \"columnDistinctCount\"\n" +
            "   val df = snappysession.table(%1)\n" +
            "   val cols = %2\n" +
            "   val df1 = df.select(json_tuple(df(\"value\"), cols : _*) )\n" +
            ";" ;
    public ResultSet getApproxCountDistinct(String tablename, List<String> cols) throws Exception {
        String sql = String.format(getApproxCountDistinct, tablename, cols);
        return getConnection().createStatement().executeQuery(sql);
    }

    /**
     * Create and store binned values for input columns to a table with suffix '_prepped'
     * Each input column must be numeric. It is first transformed to a double before binning.
     * 100 bins are created.
     * The output table has all the input columns as doubles, the corresponding binNumber column
     * and a string with the bin range.
     * NOTE: 100 bins may not be appropriate in many situations and range may not be correct (e.g. ZipCode)
     */
    String storeQuantileDiscretes = "exec scala\n" +
            "val tablename = \"%1\"\n" +
            "val inputcols = \"%2\"\n" +
            "\n" +
            "var df = snappysession.sql(s\"select $inputcols from $tablename\")\n" +
            "val inputList = inputcols.split(\",\")\n" +
            "val colsToBeDiscretized = for (c <- df.columns) yield { s\"cast($c as double) as $c\" + \"_d\"}\n" +
            "var df1 = df.selectExpr( colsToBeDiscretized: _*)\n" +
            "\n" +
            "import org.apache.spark.ml.feature.QuantileDiscretizer\n" +
            "val inputDoubles = inputcols.split(\",\").map( _ + \"_d\")\n" +
            "// All to have only 100 buckets\n" +
            "for (c <- inputDoubles) {\n" +
            "  df1 = new QuantileDiscretizer().setInputCol(c).setOutputCol(c + \"_binnum\").setNumBuckets(100).fit(df1).transform(df1)\n" +
            "}\n" +
            "val disColsSelectStr = inputDoubles.map( c => { \n" +
            "     s\"\"\"concat(min($c) over (partition by $c\"\"\" + s\"\"\"_binnum), ' to ',max($c) over (partition by $c\"\"\" + s\"\"\"_binnum)) as $c\"\"\" + \"\"\"_bin\"\"\"\n" +
            "  }).mkString (\",\")\n" +
            "\n" +
            "// we save discretized columns + everything else. TODO ... change later.\n" +
            "val allSelect = disColsSelectStr + \", *\"\n" +
            "\n" +
            "df1.createOrReplaceTempView(\"t1\")\n" +
            "val preppedData = snappysession.sql(s\"select $allSelect from t1\")\n" +
            "\n" +
            "// TODO: Before the final DF is saved, need to add 'outcome' column + other non numeric influencers .... \n" +
            "//preppedData.write.format(\"column\").mode(\"overwrite\").saveAsTable(s\"$tablename\" + \"_full_prepped\")" +
            " ;\n";
    public  void storeQuantileDiscretes(String tablename, List<String> cols) throws Exception {
        String sql = String.format(storeQuantileDiscretes, tablename, cols);
        getConnection().createStatement().executeQuery(sql);
    }
}
