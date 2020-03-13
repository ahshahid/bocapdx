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


    public BasicBatchPipeline (PipelineConfig conf) {
        this.conf = conf;
        inputURI = conf.get("inputURI");

        baseTable = conf.get("baseTable", "NULL");
        extraPredicate = conf.get("extraPredicate", "");
        classifierType = conf.get("classifier", "percentile");
        metric = ((String)conf.get("metric")).toLowerCase();

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
        return PipelineUtils.loadDataFrame(inputURI, colTypes, requiredColumns, baseTable, extraPredicate);
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
        if (inputURI.contains("jdbc")) {
            String outputTable = baseTable + "_Explained" ;
            saveDataFrameToJDBC(getConnection(), outputTable,
                    getExplanationAsDataFrame(output), true);
            SimpleExplanationNLG e = new SimpleExplanationNLG(conf, (APLExplanation) output , outputTable, metric );
            System.out.println(e.explainAsText());
        }
        return output;
    }

    private Connection getConnection() throws SQLException {
        Connection connection;
        try {
            Class.forName("io.snappydata.jdbc.ClientDriver"); // fix later
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return DriverManager.getConnection(inputURI);
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

}
