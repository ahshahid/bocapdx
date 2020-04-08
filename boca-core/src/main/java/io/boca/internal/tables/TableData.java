package io.boca.internal.tables;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import macrobase.MacroBase;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.SparkSQLIngester;
import macrobase.ingest.result.Schema;

public class TableData {
  private static final int numCatCriteria = 4;
  private static final ColumnData[] emptyColumnData = new ColumnData[0];
  private long totalRows;
  private final int workFlowId;
  public final boolean isQuery;
  private final List<List<String>> sampleRows;
  private static final double pValueThreshold = 0.1d;
  // stores the actual tablename or query
  private final String tableOrQuery;
  private final String tableOrView;
  private final Schema schema;
  private Map<Integer, ColumnData[]> sqlTypeToColumnMappings = new HashMap<>();
  private Map<String, ColumnData> columnMappings = new HashMap<>();
  private ConcurrentHashMap<String, DependencyData> kpiDependencyMap =
      new ConcurrentHashMap<>();
  private final Future<TableData> shadowTableFuture;
  private final boolean isShadowTable;
  private final Set<String> joinCols;
  private ConcurrentHashMap<String, GraphResponse> deepInsightGraphData = new ConcurrentHashMap<>();

  private static ExecutorService executorService = Executors.newCachedThreadPool();



  private static String storeQuantileDiscretes = "exec scala\n" +
      "import org.apache.spark.sql._;" +
      "import org.apache.spark.sql.types._;" +
      "import org.apache.spark.mllib.stat._;" +
      "import org.apache.spark.sql.functions._;" +
      "val tablename = \"%1$s\";" +
      "val tableDf = snappysession.table(tablename);" +
      "val schema = tableDf.schema;" +
      "val colsToModifyIndexes = Array[Int](%2$s);" +
      "val modColIndexToBuckets = Map[Int, Int](%3$s);" +
      "val newTableName = \"%4$s\";" +
      "val schemapart1 = schema.zipWithIndex.map{ case(sf, index) => {" +
      "                     if (colsToModifyIndexes.contains(index)) { " +
      "                           sf.copy(dataType = StringType);" +
      "                     } else { sf; }" +
      "                }};" +
      "val schemapart2 = schema.zipWithIndex.filter{ case(_, index) => colsToModifyIndexes.contains(index)}." +
      "unzip._1.map(sf => sf.copy(name = \""
       + MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX + "\" + sf.name, dataType = DoubleType));" +

      "val newSchema = StructType(schemapart1 ++ schemapart2);" +
      "val projectionWithDoubleCast = schema.zipWithIndex.map{ case (sf, index) => {" +
      "                     if (colsToModifyIndexes.contains(index)) { " +
      "                           s\"cast( ${sf.name} as double) as ${sf.name}\";" +
      "                     } else { sf.name; }" +
      "                }}.mkString(\",\");" +
      "val dfToOp = snappysession.sql(s\"select $projectionWithDoubleCast from $tablename\");" +
      "val dfToOpSchema =  dfToOp.schema;" +
      "import scala.math.Ordering.Implicits; " +
      "val sortedColsToModifyIndexes = colsToModifyIndexes.sorted;" +

      "import org.apache.spark.ml.feature.Bucketizer;" +
      "import org.apache.spark.ml.feature.QuantileDiscretizer;" +
      "val (preppedDf, bucketizers) = sortedColsToModifyIndexes." +
      "foldLeft[(DataFrame, Seq[Bucketizer])](dfToOp -> Seq.empty[Bucketizer])((tup, elem) => {" +
      "        val bc =  new QuantileDiscretizer().setInputCol(dfToOpSchema(elem).name)." +
      "   setOutputCol(dfToOpSchema(elem).name + \"_binnum\").setNumBuckets(modColIndexToBuckets.get(elem).get).fit(tup._1);" +
      "   val newDf = bc.transform(tup._1);" +
      "   newDf -> (tup._2 :+ (bc));" +
      "});" +
      "val furtherPreppedRdd = preppedDf.rdd.map[Row](row => {" +
      "     var j: Int = 0;" +
      "     val part2 = Array.ofDim[Any](colsToModifyIndexes.length);" +
      "     val newSeq = Seq.tabulate[Any](schema.length)(i => if (j < sortedColsToModifyIndexes.length " +
      "                             && i == sortedColsToModifyIndexes(j)) {" +
      "           val binValue = row.getDouble(schema.length + j); " +
      "           val splits = bucketizers(j).getSplits; " +
      "           part2(j) = row(i); " +
      "           j += 1;" +
      "         /* val doubl = splits(binValue.toInt);" +
      "           (doubl * 10000).round / 10000.toDouble; */  " +
      "           if (binValue.toInt < splits.length - 1) { " +
      "              s\"${splits(binValue.toInt)} - ${splits(binValue.toInt + 1)}\";" +
      "           } else {" +
      "              s\"${splits(binValue.toInt)} - \";" +
      "           }" +
      "        } else {" +
      "          row(i);" +
      "        }" +
      "    );   " +
      "    Row.fromSeq(newSeq ++ part2.toSeq);" +
      " });" +
      "val furtherPreppedDf = snappysession.sqlContext.createDataFrame(furtherPreppedRdd, newSchema);" +
      "furtherPreppedDf.write.format(\"column\").mode(\"overwrite\").saveAsTable(newTableName);" +
      " ;\n";
  private static String catToCatCorr = "import org.apache.spark.mllib.linalg._;"
      + "import org.apache.spark.sql._;"
      + "import org.apache.spark.sql.types._;"
      + "import org.apache.spark.mllib.stat._;"
      + "import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer};"
      + "import org.apache.spark.sql.functions._;"
      + "val tableDf = snappysession.table(\"%1$s\");"
      + "val pValueThreshold = %4$s;"
      + "val tableSchema = tableDf.schema;"
      + "val stringIndexer_suffix = \"_boca_index\";"
      + "val featuresCol = \"features\" + stringIndexer_suffix;"
      + "val depsColIndxs = Array[Int](%2$s);"
      + "val kpiColsIndex = %3$s;"
      + "val (depStringCols, depNonStringCols) = depsColIndxs.partition(i => tableSchema(i).dataType.equals(StringType));"
      + "val (stringColIndexers, indexcolsname) = depStringCols.map(colIndx => {"
      +     "val colName = tableSchema(colIndx).name;"
      +     "val outPutColName = colName + stringIndexer_suffix;"
      + "   new StringIndexer().setInputCol(colName).setOutputCol(outPutColName) -> outPutColName ;"
      + "}).unzip;"
      + "val preparedDf_1 = stringColIndexers.foldLeft[DataFrame](tableDf)((df, indexer) => indexer.fit(df).transform(df));"
      + "val (preparedDf, kpiIndexerCol) = if (tableSchema(kpiColsIndex).dataType.equals(StringType))"
      + "                    {"
      + "                       val kpiFtCol = tableSchema(kpiColsIndex).name + stringIndexer_suffix;"
      + "                       val kpiIndexer =  new StringIndexer().setInputCol(tableSchema(kpiColsIndex).name)."
      + "                          setOutputCol(kpiFtCol);"
      + "                       kpiIndexer.fit(preparedDf_1).transform(preparedDf_1) -> Some(kpiFtCol);"
      + "                    } else preparedDf_1 -> None;"
      + "import org.apache.spark.ml.feature.VectorAssembler;"
      + "import org.apache.spark.ml.linalg.Vectors;"
      + "val featureArray = depNonStringCols.map(i => tableSchema(i).name) ++ indexcolsname;"
      + "val assembler = new VectorAssembler().setInputCols(featureArray).setOutputCol(featuresCol);"
      + "val dfWithFeature = assembler.transform(preparedDf);"
      + "import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint};"
      + "import org.apache.spark.ml.linalg.Vector;"
      + "import org.apache.spark.mllib.linalg.{Vectors => OldVectors};"
      + "import org.apache.spark.rdd.RDD;"
      + "val input: RDD[OldLabeledPoint] ="
      + "dfWithFeature.select(col(kpiIndexerCol.getOrElse(tableSchema(kpiColsIndex).name))."
      + "cast(DoubleType), col(featuresCol)).rdd.map {"
      + "case Row(label: Double, features: Vector) =>"
      + "OldLabeledPoint(label, OldVectors.fromML(features));"
      + "};"
      + "val chiSqTestResult = Statistics.chiSqTest(input).zipWithIndex;"
      + "val features = chiSqTestResult;//.filter { case (res, _) => res.pValue < pValueThreshold};\n"
      + "val outputSchema = StructType(Seq(StructField(\"depCol\", StringType, false),"
      + "StructField(\"pValue\", DoubleType, false)));"
      + "val rows = features.map {"
      + "     case (test, index) => {"
      + "      val name = featureArray(index);"
      + "      val realName = if (name.endsWith(stringIndexer_suffix))"
      + "         name.substring(0,name.length - stringIndexer_suffix.length) else name;"
      + "       Row(realName, test.pValue);"
      + "     }"
      + "};"
      + "import scala.collection.JavaConverters._;"
      + "val valueDf = snappysession.sqlContext.createDataFrame(rows.toSeq.asJava, outputSchema);";

  private static String catToContCorr = "import org.apache.spark.sql._;"
      + "import org.apache.spark.sql.types._;"
      + "import org.apache.spark.sql.functions._;"
      + "val tableDf = snappysession.table(\"%1$s\");"
      + "val tableSchema = tableDf.schema;"
      + "val table = \"%1$s\";"
      + "val catVarName = \"%2$s\";"
      + "val contVarName = \"%3$s\";"
      + "val catMeanSql = s\"select avg($contVarName), count(*), $catVarName from $table where $catVarName is not null group by $catVarName\";"
      + "val catMeanDf = snappysession.sql(catMeanSql);"
      + "val catMeanResult = catMeanDf.collect();"
      + "val overAllMeanDf = snappysession.sql(s\"select avg($contVarName) from $table \");"
      + "val isOverAllMeanBigDec =  overAllMeanDf.schema(0).dataType.isInstanceOf[DecimalType]; "
      + "val overAllMean = if (isOverAllMeanBigDec) {overAllMeanDf.collect()(0).getDecimal(0).doubleValue();} else {overAllMeanDf.collect()(0).getDouble(0);};"
      + "val isCatMeanResultBigDec = catMeanDf.schema(0).dataType.isInstanceOf[DecimalType]; "
      + "val Ai_categoryMeanDiff = catMeanResult.map(row => {"
      +  "val catMean = if (isCatMeanResultBigDec) {row.getDecimal(0).doubleValue(); }else { row.getDouble(0);};"
      + "(catMean - overAllMean) -> row.getLong(1);"
      + " });"
      + "val SStreatmentBetweenGroups = Ai_categoryMeanDiff.foldLeft(0d)((sum,tup) => {sum + tup._1 * tup._1 * tup._2;});"
      + "val SStotal = snappysession.sql(s\"select "
      + " cast (sum(cast(($contVarName - $overAllMean) * ($contVarName - $overAllMean) as double)) as double) from $table where $catVarName is not null\").collect()(0).getDouble(0);"
      + "val SSresidual = SStotal - SStreatmentBetweenGroups;"
      + "val rsq = SStreatmentBetweenGroups / SStotal; "
      + "val outputSchema = StructType(Seq(StructField(\"rsq\", DoubleType, false)));"
      + "import scala.collection.JavaConverters._;"

      + "val valueDf = snappysession.sqlContext.createDataFrame(java.util.Collections.singletonList(Row(rsq)),"
      + " outputSchema);";


  /*
      + "import org.apache.spark.ml.feature.ChiSqSelector;"
      + "val selector = new ChiSqSelector().setNumTopFeatures(depsColIndxs.length) .setFeaturesCol(\"features\")."
      + "setLabelCol(kpiIndexerCol.getOrElse(tableSchema(kpiColsIndex).name)) .setOutputCol(\"selectedFeatures\");"
      + "val result = selector.fit(dfWithFeature).transform(dfWithFeature);"
      + "val inputSchema = StructType(Seq(tableDf.schema(%2$s), tableDf.schema(%3$s)));"
      + "import org.apache.spark.sql.catalyst.encoders.RowEncoder;"
      + "implicit val encoder = RowEncoder(inputSchema);"
      + "val inputDf = tableDf.map(row => Row(row(%2$s),row(%3$s)));"
      + "println(\"col1 =\"+ tableDf.schema(%2$s).name + \"col2 = \" + tableDf.schema(%3$s).name);"
      + "val contingency_table=inputDf.stat.crosstab(tableDf.schema(%2$s).name, tableDf.schema(%3$s).name);"
      + "contingency_table.show;"
      + "val contingency_df=contingency_table.drop(tableDf.schema(%2$s).name + \"_\" + tableDf.schema(%3$s).name);"
      + "val row_count = contingency_df.count().toInt;"
      + "val column_count = contingency_df.columns.size;"
      + "val valuesArr = contingency_df.rdd.map(row => row.toSeq).flatMap(seq => seq.map(_.asInstanceOf[Long].toDouble)).collect;"
      + "val mat: Matrix = org.apache.spark.mllib.linalg.Matrices.dense(row_count, column_count, valuesArr).transpose;"
      + "val chiRes = Statistics.chiSqTest(mat.copy);"
      + "val ouputSchema = StructType(Seq(StructField(\"degreesoffreedome\", IntegerType, false),"
      + "StructField(\"method\", StringType, false), StructField(\"nullhypothesis\", StringType, false),"
      + "StructField(\"pvalue\", DoubleType, false), StructField(\"statistic\", DoubleType, false)));"
      + "val valueDf = snappysession.sqlContext.createDataFrame(java.util.Collections.singletonList("
      + "Row(chiRes.degreesOfFreedom, chiRes.method, chiRes.nullHypothesis, chiRes.pValue, chiRes.statistic)), ouputSchema);";
      */
  private static String biserialCorr = "import org.apache.spark.mllib.linalg._;"
      + "import org.apache.spark.sql._;"
      + "import org.apache.spark.sql.types._;"
      + "import org.apache.spark.mllib.stat._;"
      + "val tableDf = snappysession.table(\"%1$s\");"
      + "var biserialValue: Any = null;"
      + "val inputVectorRDD = tableDf.rdd.map[org.apache.spark.mllib.linalg.Vector](row => {"
      + "val arr = Array(row(%2$s)-> 0,row(%3$s) -> 1);"
      + "val doubleArr = arr.map{case(elem, index) => {"
      +      "%4$s"
      +   "}};"
      +   "org.apache.spark.mllib.linalg.Vectors.dense(doubleArr);"
      +  "});"
      //+ "val elementType1 = new ObjectType(classOf[Vector]);"
      //+ "val inputDataFrame = snappysession.createDataFrame(inputRowRDD, StructType(Seq(StructField(\"feature\", elementType1, false))));"
      + "val matrix = Statistics.corr(inputVectorRDD, \"pearson\");"
      + "val corrValue = matrix(0,0);"
      + "val dfStrct = StructType(Seq(StructField(\"corr\", DoubleType, false)));"
      + "val valueDf = snappysession.sqlContext.createDataFrame(java.util.Collections.singletonList(Row(corrValue)), dfStrct);";


  private static String contiToContiCorr = "import org.apache.spark.mllib.linalg._;"
      + "import org.apache.spark.sql._;"
      + "import org.apache.spark.sql.types._;"
      + "import org.apache.spark.mllib.stat._;"
      + "import org.apache.spark.sql.functions._;"
      + "val tableDf = snappysession.table(\"%1$s\");"
      + "val tableSchema = tableDf.schema;"
      + "val depColIndexes = Array[Int](%2$s);"
      + "val kpiColIndex = %3$s;"
      + "val kpiColExpression = if (tableSchema(kpiColIndex).dataType != DoubleType) {"
      + "                          s\"cast(${tableSchema(kpiColIndex).name} as double)\";"
      + "                       } else { "
      + "                         s\"${tableSchema(kpiColIndex).name}\";"
      + "                         };"
      + "val correlationExprs = depColIndexes.map(index => {"
      + "                      val depColExpression = if (tableSchema(index).dataType != DoubleType) {"
      + "                          s\"cast(${tableSchema(index).name} as double)\";"
      + "                       } else { "
      + "                         s\"${tableSchema(index).name}\";"
      + "                       };"
      + "                      s\"corr(${depColExpression}, ${kpiColExpression})\";"
      + "}).toSeq;"
      + "val valueDf = tableDf.selectExpr(correlationExprs :_*);";

  private static String BAR_CHART = "bar";
  private static String AREA_CHART = "area";
  private static String HISTOGRAM = "histogram";
  private static String CLUSTERED_BAR_CHART = "clustered_bar";

  private static String QUERY_DEEP_METRIC_CONTI = "select count(*), avg(%1$s), %2$s from %3$s group by %2$s";
  private static String QUERY_DEEP_METRIC_CAT = "select count(*), %1$s, %2$s from %3$s group by %1$s, %2$s";
  private static ThreadLocal<SQLIngester> ingesterThreadLocal = new ThreadLocal<SQLIngester>();

  private BiFunction<ColumnData, List<ColumnData> , double[]> kpiContToCont = (kpiCd, depCds) -> {
    StringBuilder depColIndexes = new StringBuilder("");
    int kpiColIndex = -1;
    List<Schema.SchemaColumn> cols = getSchema().getColumns();

    for(ColumnData depCd : depCds) {
      inner: for (int i = 0; i < cols.size(); ++i) {
        Schema.SchemaColumn sc = cols.get(i);
        if (sc.getName().equalsIgnoreCase(depCd.name)) {
          depColIndexes.append(i).append(',');
          break inner;
        }
      }
    }
    depColIndexes.deleteCharAt(depColIndexes.length() - 1);


    for (int i = 0; i < cols.size(); ++i) {
     Schema.SchemaColumn sc = cols.get(i);
        if (sc.getName().equalsIgnoreCase(kpiCd.name)) {
          kpiColIndex = i;
          break ;
        }
    }

    String scalaCodeToExecute = String.format(contiToContiCorr,
        this.getTableOrView(), depColIndexes.toString(), kpiColIndex);
    SQLIngester ingester = ingesterThreadLocal.get();
    try {
      ResultSet rs = ingester.executeQuery("exec scala options (returnDF 'valueDf') " + scalaCodeToExecute);
      rs.next();
      double[] retVal = new double[depCds.size()];
      for(int i = 0; i < retVal.length; ++i) {
        retVal[i] = rs.getDouble(i + 1);
      }
      return retVal;
    } catch(SQLException sqle) {
      throw new RuntimeException(sqle);
    }

  };

  private BiFunction<ColumnData, ColumnData, Double> biserial = (kpiCd, depCd) -> {
    int depColIndex = -1, kpiColIndex = -1;
    List<Schema.SchemaColumn> cols = getSchema().getColumns();
    for(int i = 0 ; i < cols.size(); ++i) {
      Schema.SchemaColumn sc = cols.get(i);
      if (depColIndex == -1 ||  kpiColIndex == -1) {
        if (sc.getName().equalsIgnoreCase(kpiCd.name)) {
          kpiColIndex = i;
        } else if (sc.getName().equalsIgnoreCase(depCd.name)) {
          depColIndex = i;
        }
      } else {
        break;
      }
    }

      int indexOfCatCol = -1;
      if (depCd.ft.equals(FeatureType.categorical)) {
        assert depCd.numDistinctValues == 2;
        indexOfCatCol = 0;
      } else {
        assert kpiCd.numDistinctValues == 2;
        indexOfCatCol = 1;
      }
      String elementTransformationCode = " if (biserialValue == null && index == " + indexOfCatCol +") biserialValue = elem;"
          + "val isCatCol = index == " + indexOfCatCol + ";"
          +" elem match {"
          +     "case x: Short => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else x.toDouble;"
          +     "case x: Int => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else x.toDouble;"
          +     "case x: Long => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else x.toDouble;"
          +     "case x: Float => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else x.toDouble;"
          +     "case x: Double => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else x;"
          +     "case x: Decimal => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else x.toDouble;"
          +     "case x: String => if (isCatCol) if(elem == biserialValue) 0.0d else 1.0d else throw new RuntimeException(\"continuous var as string\");"
          +     "case _ => throw new RuntimeException(\"unknown type\");"
          +     "}";


    String scalaCodeToExecute = String.format(biserialCorr,
        this.getTableOrView(), depColIndex, kpiColIndex, elementTransformationCode);
    SQLIngester ingester = ingesterThreadLocal.get();
    try {
      ResultSet rs = ingester.executeQuery("exec scala options (returnDF 'valueDf') " + scalaCodeToExecute);
      rs.next();
      return rs.getDouble(1);
    } catch(SQLException sqle) {
      throw new RuntimeException(sqle);
    }

  };


  private BiFunction<ColumnData, ColumnData, Double> catToCont = (catCol, contCol) -> {
    String scalaCodeToExecute = String.format(catToContCorr,
        this.getTableOrView(), catCol.name, contCol.name);
    SQLIngester ingester = ingesterThreadLocal.get();
    try {
      ResultSet rs = ingester.executeQuery("exec scala options (returnDF 'valueDf') " + scalaCodeToExecute);
      rs.next();
      return rs.getDouble(1);
    } catch(SQLException sqle) {
      throw new RuntimeException(sqle);
    }

  };


  private BiFunction<ColumnData, List<ColumnData>, Map<String, Double>> categoricalToCategorical = (kpiCd, depCds) -> {
    List<Schema.SchemaColumn> cols = getSchema().getColumns();

    int kpiColIndex = -1;
    List<Integer> depColsIndex = new ArrayList<>();


    for(int i = 0 ; i < cols.size(); ++i) {
      Schema.SchemaColumn sc = cols.get(i);
      if (kpiColIndex == -1) {
        if (sc.getName().equalsIgnoreCase(kpiCd.name)) {
          kpiColIndex = i;
        } else if (depCds.stream().anyMatch(elem -> elem.name.equalsIgnoreCase(sc.getName()))) {
          depColsIndex.add(i);
        }
      } else  if (depCds.stream().anyMatch(elem -> elem.name.equalsIgnoreCase(sc.getName()))) {
          depColsIndex.add(i);
      }
    }
    StringBuilder sb = new StringBuilder();
    depColsIndex.stream().forEach(i -> sb.append(i).append(','));
    sb.deleteCharAt(sb.length() -1);
    String depsColindxsStr = sb.toString();
    String scalaCodeToExecute = String.format(catToCatCorr,
        this.getTableOrView(), depsColindxsStr, kpiColIndex, String.valueOf(pValueThreshold));
    SQLIngester ingester = ingesterThreadLocal.get();
    try {
      ResultSet rs = ingester.executeQuery("exec scala options (returnDF 'valueDf') " + scalaCodeToExecute);
      Map<String, Double> corrData = new HashMap<>();
      while(rs.next()) {
        corrData.put(rs.getString(1), rs.getDouble(2));
      }
      return corrData;
    } catch(SQLException sqle) {
      throw new RuntimeException(sqle);
    }

  };

  private Function<String, DependencyData> depedencyComputer = kpi -> {

    ColumnData kpiCol = columnMappings.get(kpi.toLowerCase());
    DependencyData dd = new DependencyData(kpiCol.name, kpiCol.ft);
    if (kpiCol.ft.equals(FeatureType.continuous)) {
      // identify which are continuous or categorical cols which can use pearson corr directly
      List<ColumnData> pearsonAmenable = columnMappings.values().stream().filter(cd ->
         !cd.name.equalsIgnoreCase(kpi) && !cd.skip && (cd.ft.equals(FeatureType.continuous) ||
             (cd.ft.equals(FeatureType.categorical) && cd.numDistinctValues == 2 && (cd.sqlType == Types.SMALLINT ||
                 cd.sqlType == Types.INTEGER || cd.sqlType == Types.BIGINT || cd.sqlType == Types.DOUBLE
          || cd.sqlType == Types.FLOAT)))
      ).collect(Collectors.toList());
      if (!pearsonAmenable.isEmpty()) {
        double[] corrs = kpiContToCont.apply(kpiCol, pearsonAmenable);
        for(int i = 0 ; i < corrs.length; ++i) {
          dd.addToPearson(pearsonAmenable.get(i).name, corrs[i]);
        }
      }

      List<ColumnData> annovaAmenable = columnMappings.values().stream().filter(cd ->
          !cd.name.equalsIgnoreCase(kpi) && !cd.skip && cd.ft.equals(FeatureType.categorical) &&
              cd.numDistinctValues != 2 ).collect(Collectors.toList());
      if (!annovaAmenable.isEmpty()) {
        for(ColumnData cd: annovaAmenable) {
          double corr = catToCont.apply(cd, kpiCol);
          dd.addToAnova(cd.name, corr);
        }
      }

      List<ColumnData> biserialAmenable = columnMappings.values().stream().filter(cd ->
          !cd.name.equalsIgnoreCase(kpi) && !cd.skip &&
              cd.ft.equals(FeatureType.categorical) && cd.numDistinctValues == 2 && !(cd.sqlType == Types.SMALLINT ||
                  cd.sqlType == Types.INTEGER || cd.sqlType == Types.BIGINT || cd.sqlType == Types.DOUBLE
                  || cd.sqlType == Types.FLOAT)
      ).collect(Collectors.toList());
      if (!biserialAmenable.isEmpty()) {
        for(ColumnData cd: biserialAmenable) {
          double corr = biserial.apply(cd, kpiCol);
          dd.addToPearson(cd.name, corr);
        }
      }

    } else if (kpiCol.ft.equals(FeatureType.categorical)) {
      // get all the categorical cols
      List<ColumnData> deps = columnMappings.values().stream().filter(ele -> !ele.name.equalsIgnoreCase(kpi)
          && !ele.skip && ele.ft.equals(FeatureType.categorical)).collect(Collectors.toList());
      Map<String, Double> corrData = categoricalToCategorical.apply(kpiCol, deps);
      for(Map.Entry<String, Double> entry: corrData.entrySet()) {
        dd.addToChiSqCorrelation(entry.getKey(), entry.getValue());
      }
      // get all the continous cols
      List<ColumnData> contiCols = columnMappings.values().stream().filter(ele -> !ele.name.equalsIgnoreCase(kpi)
          && !ele.skip && ele.ft.equals(FeatureType.continuous)).collect(Collectors.toList());
      if (!contiCols.isEmpty()) {
        if (kpiCol.numDistinctValues == 2) {
          if (kpiCol.sqlType == Types.SMALLINT ||
              kpiCol.sqlType == Types.INTEGER || kpiCol.sqlType == Types.BIGINT || kpiCol.sqlType == Types.DOUBLE
              || kpiCol.sqlType == Types.FLOAT) {
            double[] corrs = kpiContToCont.apply(kpiCol, contiCols);
            for (int i = 0; i < corrs.length; ++i) {
              dd.addToPearson(contiCols.get(i).name, corrs[i]);
            }
          } else {
            // biserial
            contiCols.stream().forEach(contiCol -> {
              double corr = biserial.apply(kpiCol, contiCol);
              dd.addToPearson(contiCol.name, corr);
            });
          }
        } else {
          contiCols.stream().forEach(contiCol -> {
            double corr = catToCont.apply(kpiCol, contiCol);
            dd.addToAnova(contiCol.name, corr);
          });
        }
      }

    }
    return dd;
  };


  private Function<String, GraphResponse> deepInsightGraphComputer = graphKey -> {
    String featureCol = graphKey.substring(0, graphKey.indexOf("||"));
    String metricCol = graphKey.substring(graphKey.indexOf("||") + "||".length());
    return getDeepInsightGraphResponse(featureCol, metricCol, ingesterThreadLocal.get());
  };

   GraphResponse getDeepInsightGraphResponse(String feature, String metric, SQLIngester ingester) {
    GraphResponse response = null ;
    try {
      int actualFeatureColType = getColumnData(feature).sqlType;
      final TableData preppedTableData = getTableDataForFastInsights();
      TableData.ColumnData metricColCd = getColumnData(metric);
      List<Schema.SchemaColumn> actualCols = preppedTableData.getSchema().getColumns().stream().
              filter(sc -> sc.getName().
                      equalsIgnoreCase(MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX + metric)).
              collect(Collectors.toList());
      String metricCol = metric;
      boolean metricColShadowExists = !actualCols.isEmpty();
      if (metricColShadowExists) {
        metricCol = actualCols.get(0).getName();
      }
      String tableName = preppedTableData.getTableOrView();
      response = getResponseForDeepInsights(tableName, feature, actualFeatureColType,
              metric, metricCol, ingester, metricColCd);

      return response;
    } catch(Exception e) {
      throw new RuntimeException(e);
    }

  }

  private static GraphResponse getResponseForDeepInsights(String tableName, String featureCol,int actualFeatureColType,
                                                          String metricCol,
                                                          String unbinnedMetricCol, SQLIngester ingester,
                                                  TableData.ColumnData metricColCd)  {
     try{
    if (!metricColCd.skip && metricColCd.ft.equals(FeatureType.continuous)) {
      String query = String.format(QUERY_DEEP_METRIC_CONTI, unbinnedMetricCol, featureCol, tableName);
      ResultSet rs = ingester.executeQuery(query);
      int featureColType = rs.getMetaData().getColumnType(3);
      List<GraphResponse.GraphPoint> dataPoints = new LinkedList<>();
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

        GraphResponse.GraphPoint gp = new GraphResponse.GraphPoint(count, key, String.valueOf(avg), lb, ub);
        dataPoints.add(gp);
        ++numRows;
      }

      GraphResponse gr = new GraphResponse();
      gr.isFeatureNumeric = isFeatureNumeric;
      gr.isFeatureRange = isFeatureRange;
      gr.isMetricNumeric = isMetricNumeric;
      gr.dataPoints = dataPoints;
      gr.graphType = numRows > 50 ? AREA_CHART : isFeatureRange ? HISTOGRAM : BAR_CHART;
      return gr;

    } else if (!metricColCd.skip && metricColCd.ft.equals(FeatureType.categorical)) {
      String query = String.format(QUERY_DEEP_METRIC_CAT, featureCol, metricCol, tableName);
      ResultSet rs = ingester.executeQuery(query);
      int featureColType = rs.getMetaData().getColumnType(2);
      int metricColType = rs.getMetaData().getColumnType(3);
      List<GraphResponse.GraphPoint> dataPoints = new LinkedList<>();
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

        if (featureColType == Types.VARCHAR) {
          key = rs.getString(2);
          if (isFeatureNumeric && isFeatureRange) {
            lb = key.substring(0, key.indexOf(" - ")).trim();
            ub = key.substring(key.indexOf(" - ")).trim();
          }
        } else {
          key = rs.getObject(2).toString();
        }

        String kpi = rs.getObject(3).toString();

        GraphResponse.GraphPoint gp = new GraphResponse.GraphPoint(count, key, kpi, lb, ub);
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
    return null;
  } catch (SQLException sqle) {
     throw new RuntimeException(sqle);
  }
  }

  TableData(String tableOrQuery, SQLIngester ingester, int workFlowId, boolean isQuery, Set<String> joinCols) throws SQLException, IOException {
    this(tableOrQuery, ingester, workFlowId, isQuery,false, joinCols);
  }
  TableData(String tableOrQuery, SQLIngester ingester, int workFlowId, boolean isQuery,
      boolean isShadowTable, Set<String> joinCols)
      throws SQLException, IOException {
    this.isShadowTable = isShadowTable;
    this.joinCols = joinCols;
    // filter columns of interest.
    // figure out if they are continuous or categorical
    this.workFlowId = workFlowId;
    this.isQuery = isQuery;
    this.tableOrQuery = tableOrQuery;
    // if query create a view
    if (isQuery) {
      String viewName = MacroBaseDefaults.BOCA_VIEWS_PREFIX + workFlowId;
      ingester.executeSQL("drop table if exists " + viewName );
      String viewDef = "create table " + viewName + " as (" + tableOrQuery + ")";
      ingester.executeSQL(viewDef);
      this.tableOrView = viewName;
    } else {
      this.tableOrView = tableOrQuery;
    }

    String query = "select * from " + this.tableOrView + " limit 100;";
    ResultSet sampleSet = ingester.executeQuery(query);
    this.schema = ingester.getSchema(sampleSet);
    this.sampleRows = Utils.convertResultsetToRows(sampleSet);
    // avoid it for shadow table
    // get total rows
    String countQuery = "select count (*) from " + tableOrView;
    ResultSet rs = ingester.executeQuery(countQuery);
    rs.next();
    totalRows = rs.getLong(1);
    // segregate columns based on sql types
    Map<Integer, List<Schema.SchemaColumn>> groups =  schema.getColumns().stream().
        filter(sc -> !sc.getName().startsWith(MacroBaseDefaults.BOCA_SHADOW_TABLE_UNBINNED_COL_PREFIX)).
        collect(Collectors.groupingBy(sc -> sc.sqlType()));

    for(Map.Entry<Integer, List<Schema.SchemaColumn>> entry : groups.entrySet()) {
       ColumnData[] cds = determineColumnData(entry.getKey(), entry.getValue(),
           ingester);
      sqlTypeToColumnMappings.put(entry.getKey(), cds);
      for(ColumnData cd: cds) {
        columnMappings.put(cd.name.toLowerCase(), cd);
      }
    }

    if (isShadowTable) {
      this.shadowTableFuture = null;
    } else {
      // check if the table contains int / big int/ doube/float columns which are continuous & not primary key ones.
      // if yes we may have to create a shadow prepped table which is binned.
      List<ColumnData> columnsNeedingMod = new ArrayList<ColumnData>();
      for (ColumnData cd : this.sqlTypeToColumnMappings.getOrDefault(Types.INTEGER, emptyColumnData)) {
        if (!cd.skip && cd.ft.equals(FeatureType.continuous)) {
          columnsNeedingMod.add(cd);
        }
      }

      for (ColumnData cd : this.sqlTypeToColumnMappings.getOrDefault(Types.BIGINT, emptyColumnData)) {
        if (!cd.skip && cd.ft.equals(FeatureType.continuous)) {
          columnsNeedingMod.add(cd);
        }
      }

      for (ColumnData cd : this.sqlTypeToColumnMappings.getOrDefault(Types.FLOAT, emptyColumnData)) {
        if (!cd.skip && cd.ft.equals(FeatureType.continuous)) {
          columnsNeedingMod.add(cd);
        }
      }

      for (ColumnData cd : this.sqlTypeToColumnMappings.getOrDefault(Types.DOUBLE, emptyColumnData)) {
        if (!cd.skip && cd.ft.equals(FeatureType.continuous)) {
          columnsNeedingMod.add(cd);
        }
      }
      if (columnsNeedingMod.isEmpty()) {
        this.shadowTableFuture = null;
      } else {
        this.shadowTableFuture = executorService.submit(getShadowTableCreationTask(columnsNeedingMod, ingester));
      }
    }

  }

  private Callable<TableData> getShadowTableCreationTask(final List<ColumnData> colsToBin,
      final SQLIngester ingester) {
    Callable<TableData> callableTask = new Callable<TableData>() {
      @Override
      public TableData call() throws Exception {
        String shadowTableName = MacroBaseDefaults.BOCA_SHADOW_TABLE_PREFIX + TableData.this.getTableOrView();
        StringBuilder modColIndexesBuff = new StringBuilder("");
        int[] modColIndexes = new int[colsToBin.size()];
        List<Schema.SchemaColumn> cols = getSchema().getColumns();
        int j = 0;
        for (ColumnData cd : colsToBin) {
          inner:
          for (int i = 0; i < cols.size(); ++i) {
            Schema.SchemaColumn sc = cols.get(i);
            if (sc.getName().equalsIgnoreCase(cd.name)) {
              modColIndexes[j++] = i;
              modColIndexesBuff.append(i).append(',');
              break inner;
            }
          }
        }
        modColIndexesBuff.deleteCharAt(modColIndexesBuff.length() - 1);
        StringBuilder bucketsMapping = new StringBuilder();
        j = 0;
        for (ColumnData cd : colsToBin) {
          long range = cd.approxMax - cd.approxMin;
          long numBuckets = 2;
          if (range <= 10) {
            numBuckets = 10;
          } else if (range <= 100) {
            numBuckets = 100;
          } else if (range < 1000) {
            numBuckets = 1000;
          } else {
            numBuckets = range /100 ;
          }
          if (numBuckets > 1000000) {
            numBuckets = 1000;
          }
          //numBuckets = 50;
         /* long criteria = (TableData.this.totalRows * (numCatCriteria- 1)) / 100;
          // range exceeds the max distinct criteria by
          long excess = range - criteria;*/

          bucketsMapping.append(modColIndexes[j]).append("->").append((int)numBuckets).append(',');
          ++j;
        }
        bucketsMapping.deleteCharAt(bucketsMapping.length() - 1);

        String createDiscretizedTable = String.format(storeQuantileDiscretes,
            TableData.this.getTableOrView(), modColIndexesBuff.toString(), bucketsMapping.toString(),
            shadowTableName);
        ingester.executeSQL("drop table if exists " + shadowTableName);
        ingester.executeQuery(createDiscretizedTable);
        return new TableData(shadowTableName, ingester, -1, false,
            true, TableData.this.joinCols);

      }
    } ;
    return callableTask;

  }

  public long getTotalRowsCount() {
    return  this.totalRows;
  }

  public long getWorkFlowId() {
    return this.workFlowId;
  }



  public Schema getSchema() {
    return this.schema;
  }

  public GraphResponse getDeepInsightGraphData(String metricCol, String featureCol,
                                               SQLIngester ingester) throws Exception {
    ingesterThreadLocal.set(ingester);
    String graphKey = featureCol + "||" + metricCol;
    return deepInsightGraphData.computeIfAbsent(graphKey, deepInsightGraphComputer );
  }

  public GraphResponse getDeepInsightGraphData(String metricCol, String featureCol,
                                               Connection conn)  {
     try {
       SQLIngester ingester = new SparkSQLIngester(conn);
       ingesterThreadLocal.set(ingester);
       String graphKey = featureCol + "||" + metricCol;
       return deepInsightGraphData.computeIfAbsent(graphKey, deepInsightGraphComputer);
     } catch(Exception e) {
       throw new RuntimeException(e);
     }
  }

  public ColumnData getFirstAvailablePkColumn() {
    Optional<ColumnData> opt =this.columnMappings.values().stream().filter(cd -> cd.possiblePrimaryKey).findFirst();
    return opt.orElseThrow(() -> new RuntimeException("Primary Key not found"));
  }

  public ColumnData getFirstAvailablePkColumn(int sqlType) {
    ColumnData[] arr = this.sqlTypeToColumnMappings.getOrDefault(sqlType, new ColumnData[0]);
    for(ColumnData cd : arr) {
      if (cd.isPossiblePrimaryKey()) {
        return cd;
      }
    }
    throw new  RuntimeException("Primary Key not found");
  }

  public ColumnData getColumnData(String colName) {
    return this.columnMappings.get(colName);
  }

  public String getTableOrView() {
    return this.tableOrView;
  }

  public List<List<String>> getSampleRows() {
    return Collections.unmodifiableList(this.sampleRows);
  }

  public DependencyData getDependencyData(String kpiColumn, SQLIngester ingester) {
    ingesterThreadLocal.set(ingester);
    return kpiDependencyMap.computeIfAbsent(kpiColumn, depedencyComputer);
  }

  public TableData getTableDataForFastInsights() throws Exception {
    if (this.shadowTableFuture != null) {
      return this.shadowTableFuture.get();
    } else {
      return this;
    }
  }

  private ColumnData[] determineColumnData(int sqlType, List<Schema.SchemaColumn> scs,
      SQLIngester ingester)
  throws SQLException {
    ColumnData[] cds = new ColumnData[scs.size()];
    switch (sqlType) {
      case Types.INTEGER:
      case Types.BIGINT: {
        String countClause = scs.stream().map(sc -> sc.getName()).reduce("", (str1, str2)
            -> str1 + "," + " approx_count_distinct(" + str2 + "), max(" + str2 + "), min (" + str2 + ")").substring(1);
        String query = "select  %1$s from %2$s";

        ResultSet rs = ingester.executeQuery(String.format(query, countClause, this.tableOrView));
        rs.next();
        List<Integer> possiblePkIndexes = new ArrayList<>(cds.length);
        int i = 1;
        int k = 0;
        for (Schema.SchemaColumn sc : scs) {
          long distinctValues = rs.getLong(i);
          long max = sqlType == Types.INTEGER ? rs.getInt(i + 1): rs.getLong(i + 1);
          long min = sqlType == Types.INTEGER ? rs.getInt(i + 2): rs.getLong(i + 2);
          boolean skip = false;
          FeatureType ft = FeatureType.unknown;
          int percent = totalRows > 0 ?(int)((100 * distinctValues) / totalRows): -1;
          if (percent != -1) {
            if (percent > 80) {
              //skip = true;
              skip = false;
              ft =  this.isShadowTable ? FeatureType.categorical : FeatureType.continuous;
            } else if (percent < numCatCriteria ) {
              ft =   FeatureType.categorical ;
              skip = false;
            } else {
              skip = false;
              ft =  this.isShadowTable ? FeatureType.categorical : FeatureType.continuous;
            }
          } else {
            skip = true;
          }
          // if approx distinct values is > 90 % of total rows , get exact figure
          if (distinctValues > (90 * totalRows)/100) {
            if (!(isQuery || isShadowTable)) {
              possiblePkIndexes.add(k);
            }
          }
          skip = skip || joinCols.contains(sc.getName());
          cds[k] = new ColumnData(sc.getName().toLowerCase(), ft, skip, sqlType,
              distinctValues, false, max, min);
          i += 3;
          ++k;
        }
        if (!possiblePkIndexes.isEmpty()) {
          String exactCountClause = possiblePkIndexes.stream().map(j -> cds[j].name).reduce("", (str1, str2)
              -> str1 + "," + " count(distinct " + str2 + ")").substring(1);

          rs = ingester.executeQuery(String.format(query, exactCountClause, this.tableOrView));
          rs.next();
          i = 1;
          for (int pos : possiblePkIndexes) {
            ColumnData cdd = cds[pos];
            cdd.numDistinctValues = rs.getLong(i);
            cdd.possiblePrimaryKey = cdd.numDistinctValues == this.totalRows;
            ++i;
          }
        }

        return cds;
      }
      case Types.DECIMAL:
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE: {
        String countClause = scs.stream().map(sc -> sc.getName()).reduce("", (str1, str2)
            -> str1 + "," + " approx_count_distinct(" + str2 + "), max(" + str2 + "), min (" + str2 + ")").substring(1);
        String query = "select  %1$s from %2$s";

        ResultSet rs = ingester.executeQuery(String.format(query, countClause, this.tableOrView));
        rs.next();
        int i = 1;
        int k = 0;
        for (Schema.SchemaColumn sc : scs) {
          long distinctValues = rs.getLong(i);
          long max = Integer.MIN_VALUE;
          long min = Integer.MAX_VALUE;
          if (sqlType == Types.DECIMAL) {
            max = rs.getBigDecimal(i + 1).longValue();
            min = rs.getBigDecimal(i + 2).longValue();
          } else if (sqlType == Types.FLOAT || sqlType == Types.REAL) {
            max = (long)rs.getFloat(i + 1);
            min = (long)rs.getFloat(i + 2);
          } else if (sqlType == Types.DOUBLE) {
            max = (long)rs.getDouble(i + 1);
            min = (long)rs.getDouble(i + 2);
          }
          boolean skip = false;
          FeatureType ft = FeatureType.unknown;
          int percent = totalRows > 0 ?(int)((100 * distinctValues) / totalRows): -1;
          if (percent != -1) {
            if (percent > 80) {
              //skip = true;
              skip = false;
              ft =  this.isShadowTable ? FeatureType.categorical : FeatureType.continuous;
            } else if (percent < numCatCriteria ) {
              ft =   FeatureType.categorical;
              skip = false;
            } else {
              skip = false;
              ft =  this.isShadowTable ? FeatureType.categorical : FeatureType.continuous;
            }
          } else {
            skip = true;
          }
          skip = skip || joinCols.contains(sc.getName());
          cds[k] = new ColumnData(sc.getName().toLowerCase(), ft, skip, sqlType,
              distinctValues, false, max, min);
          i += 3;
          ++k;
        }
        return cds;

      }


     /* case Types.NCHAR:
      case Types.CHAR:{
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName().toLowerCase(), FeatureType.categorical, false,
              sqlType);
        }
        return cds;
      } */
      case Types.NCHAR:
      case Types.CHAR:
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
      {
        List<Integer> possiblePkIndexes = new ArrayList<>(cds.length);

        String countClause = scs.stream().map(sc -> sc.getName().toLowerCase()).reduce("", (str1, str2)
            -> str1 + "," + " approx_count_distinct(" + str2 + ")").substring(1);
        String query = "select  %1$s from %2$s";
        ResultSet rs = ingester.executeQuery(String.format(query, countClause, this.tableOrView));
        rs.next();
        int i = 1;
        for (Schema.SchemaColumn sc : scs) {
          long distinctCount = rs.getLong(i);
          boolean skip = false;
          FeatureType ft = FeatureType.unknown;
          int percent = totalRows > 0 ?(int)((100 * distinctCount) / totalRows): -1;
          if (percent != -1) {
            if (percent > 80) {
              skip = true;
            } else {
              ft = FeatureType.categorical;
              skip = false;
            }
          } else {
            skip = true;

          }
          // if approx distinct values is > 90 % of total rows , get exact figure
          if (distinctCount > (90 * totalRows)/100) {
            if (!isQuery) {
              possiblePkIndexes.add(i - 1);
            }
          }
          skip = skip || joinCols.contains(sc.getName());
          cds[i - 1] = new ColumnData(sc.getName().toLowerCase(), ft, skip, sqlType,
              distinctCount, false, Integer.MIN_VALUE, Integer.MAX_VALUE);
          ++i;
        }

        if (!possiblePkIndexes.isEmpty()) {
          String exactCountClause = possiblePkIndexes.stream().map(j -> cds[j].name).reduce("", (str1, str2)
              -> str1 + "," + " count(distinct " + str2 + ")").substring(1);

          rs = ingester.executeQuery(String.format(query, exactCountClause, this.tableOrView));
          rs.next();
          i = 1;
          for (int pos : possiblePkIndexes) {
            ColumnData cdd = cds[pos];
            cdd.numDistinctValues = rs.getLong(i);
            cdd.possiblePrimaryKey = cdd.numDistinctValues == this.totalRows;
            ++i;
          }
        }
        return cds;
      }
      default: {
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName().toLowerCase(), null, true,
              sqlType, -1, false, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
        return cds;
      }

    }
  }

  public static class ColumnData {
    final public FeatureType ft;
    final public boolean skip;
    final public int sqlType;
    final public String name;
    private long numDistinctValues;
    private boolean possiblePrimaryKey;
    // looses teh decimal part;
    final private long approxMax;
    final private long approxMin;


    ColumnData(String name, FeatureType ft, boolean skip, int sqlType,
        long numDistinctValues, boolean possiblePrimaryKey, long approxMax, long approxMin) {
      this.ft = ft;
      this.skip = skip;
      this.sqlType = sqlType;
      this.name = name;
      this.numDistinctValues = numDistinctValues;
      this.possiblePrimaryKey = possiblePrimaryKey;
      this.approxMax = approxMax;
      this.approxMin = approxMin;
    }

    public boolean isPossiblePrimaryKey() {
      return this.possiblePrimaryKey;
    }
  }

}
