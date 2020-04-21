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

import com.google.common.collect.Maps;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableCell;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.*;
import com.google.visualization.datasource.render.JsonRenderer;
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
  private ConcurrentHashMap<String, DataTable> deepInsightGraphData = new ConcurrentHashMap<>();

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
      "import org.apache.spark.sql.functions._; " +
      "val sortedColsToModifyIndexes = colsToModifyIndexes.sorted;" +
      "val sortedColNames = sortedColsToModifyIndexes.map(pos => dfToOpSchema(pos).name);" +
      "val minMaxColExprs =  sortedColNames.flatMap(colName => Array(min(dfToOp.col(colName)), max(dfToOp.col(colName)) ));" +
      "val minMaxRow = dfToOp.agg(minMaxColExprs.head, minMaxColExprs.tail: _*).collect()(0);" +
      "import org.apache.spark.ml.feature.Bucketizer;" +
      "import org.apache.spark.ml.feature.QuantileDiscretizer;" +
      "val (preppedDf, bucketizers) = sortedColsToModifyIndexes." +
      "foldLeft[(DataFrame, Seq[Bucketizer])](dfToOp -> Seq.empty[Bucketizer])((tup, elem) => {" +
      "        val bc =  new QuantileDiscretizer().setInputCol(dfToOpSchema(elem).name)." +
      "   setOutputCol(dfToOpSchema(elem).name + \"_binnum\").setNumBuckets(modColIndexToBuckets.get(elem).get).fit(tup._1);" +
      "   bc.setHandleInvalid(\"keep\");" +
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
      "         /* val doubl = splits(binValue.toInt);" +
      "           (doubl * 10000).round / 10000.toDouble; */  " +
      "             val lb = if (binValue.toInt == 0) {" +
      "                   minMaxRow.getDouble(j*2); " +
      "                } else {" +
      "                   splits(binValue.toInt);" +
      "                };" +
      "              var ub = if (binValue.toInt == splits.length - 2) {" +
      "                          minMaxRow.getDouble(2*j + 1);  " +
      "                       } else {" +
      "                          splits(binValue.toInt + 1);" +
      "                        };" +
      "                      if (binValue.toInt == splits.length-1) {" +
      "                        throw new RuntimeException(\"unexpected bin count\");" +
      "                       };" +
      "           /*   if (lb.isNegInfinity) {" +
      "                  lb = minMaxRow.getDouble(2*j);" +
      "               }; " +
      "              if (ub.isPosInfinity) {" +
      "                  ub = minMaxRow.getDouble(2*j + 1);" +
      "               }; */" +
      "              j += 1;" +
      "              s\"$lb - $ub\";" +
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
      + "val overAllMean: Double = %4$s;"
      + "val SStotal: Double = %5$s;"
      + "val isCatMeanResultBigDec = catMeanDf.schema(0).dataType.isInstanceOf[DecimalType]; "
      + "val Ai_categoryMeanDiff = catMeanResult.map(row => {"
      +  "val catMean = if (isCatMeanResultBigDec) {row.getDecimal(0).doubleValue(); }else { row.getDouble(0);};"
      + "(catMean - overAllMean) -> row.getLong(1);"
      + " });"
      + "val SStreatmentBetweenGroups = Ai_categoryMeanDiff.foldLeft(0d)((sum,tup) => {sum + tup._1 * tup._1 * tup._2;});"
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


  private QuadFunction<ColumnData, ColumnData, Double, Double, Double> catToCont = (catCol, contCol, overallAvgIfAvail,
                                                                                    ssTotIfAvail) -> {
    String _4thParam = "";
    String _5thParam = "";
    if (overallAvgIfAvail.isNaN()) {
      _4thParam =    "{"
                        + "val overAllMeanDf = snappysession.sql(s\"select avg($contVarName) from $table \");"
                        + "val isOverAllMeanBigDec =  overAllMeanDf.schema(0).dataType.isInstanceOf[DecimalType]; "
                        + "if (isOverAllMeanBigDec) {overAllMeanDf.collect()(0).getDecimal(0).doubleValue();} else {overAllMeanDf.collect()(0).getDouble(0);};"
                    + "}";
      _5thParam = "snappysession.sql(s\"select "
              + " cast (sum(cast(($contVarName - $overAllMean) * ($contVarName - $overAllMean) as double)) as double) from $table \").collect()(0).getDouble(0);";

    } else {
      _4thParam = String.valueOf(overallAvgIfAvail);
      _5thParam = String.valueOf(ssTotIfAvail);
    }
    String scalaCodeToExecute = String.format(catToContCorr,
        this.getTableOrView(), catCol.name, contCol.name, _4thParam, _5thParam);
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
    try {
      ColumnData kpiCol = columnMappings.get(kpi.toLowerCase());
      DependencyData dd = new DependencyData(kpiCol.name, kpiCol.ft);
      if (kpiCol.ft.equals(FeatureType.continuous)) {
        // identify which are continuous or categorical cols which can use pearson corr directly
        List<ColumnData> pearsonAmenable = columnMappings.values().stream().filter(cd ->
                !cd.possiblePrimaryKey && !cd.name.equalsIgnoreCase(kpi) && !cd.skip && (cd.ft.equals(FeatureType.continuous) ||
                        (cd.ft.equals(FeatureType.categorical) && cd.numDistinctValues == 2 && (cd.sqlType == Types.SMALLINT ||
                                cd.sqlType == Types.INTEGER || cd.sqlType == Types.BIGINT || cd.sqlType == Types.DOUBLE
                                || cd.sqlType == Types.FLOAT)))
        ).collect(Collectors.toList());
        if (!pearsonAmenable.isEmpty()) {
          double[] corrs = kpiContToCont.apply(kpiCol, pearsonAmenable);
          for (int i = 0; i < corrs.length; ++i) {
            dd.addToPearson(pearsonAmenable.get(i).name, corrs[i]);
          }
        }

        List<ColumnData> annovaAmenable = columnMappings.values().stream().filter(cd ->
                !cd.possiblePrimaryKey && !cd.name.equalsIgnoreCase(kpi) && !cd.skip && cd.ft.equals(FeatureType.categorical) &&
                        cd.numDistinctValues != 2).collect(Collectors.toList());
        if (!annovaAmenable.isEmpty()) {
          SQLIngester ingester = ingesterThreadLocal.get();
          String tab = this.getTableOrView();
          String q1 = "select avg(%1$s) from %2$s";
          ResultSet rs = ingester.executeQuery(String.format(q1, kpiCol.name, tab));
          int colType = rs.getMetaData().getColumnType(1);
          rs.next();
          double overAllMean;
          if (colType == Types.DECIMAL) {
            overAllMean = rs.getBigDecimal(1).doubleValue();
          } else {
            overAllMean = rs.getDouble(1);
          }

          String q2 = "select  cast (sum(cast((%1$s - %2$f) * (%1$s - %2$f) as double)) as double) from %3$s";
          rs = ingester.executeQuery(String.format(q2, kpiCol.name, overAllMean, tab));
          rs.next();
          double ssTotal = rs.getDouble(1);

          for (ColumnData cd : annovaAmenable) {
            double corr = catToCont.apply(cd, kpiCol, overAllMean, ssTotal);
            dd.addToAnova(cd.name, corr);
          }
        }

        List<ColumnData> biserialAmenable = columnMappings.values().stream().filter(cd ->
                !cd.possiblePrimaryKey && !cd.name.equalsIgnoreCase(kpi) && !cd.skip &&
                        cd.ft.equals(FeatureType.categorical) && cd.numDistinctValues == 2 && !(cd.sqlType == Types.SMALLINT ||
                        cd.sqlType == Types.INTEGER || cd.sqlType == Types.BIGINT || cd.sqlType == Types.DOUBLE
                        || cd.sqlType == Types.FLOAT)
        ).collect(Collectors.toList());
        if (!biserialAmenable.isEmpty()) {
          for (ColumnData cd : biserialAmenable) {
            double corr = biserial.apply(cd, kpiCol);
            dd.addToPearson(cd.name, corr);
          }
        }

      } else if (kpiCol.ft.equals(FeatureType.categorical)) {
        // get all the categorical cols
        List<ColumnData> deps = columnMappings.values().stream().filter(ele -> !ele.name.equalsIgnoreCase(kpi)
                && !ele.possiblePrimaryKey && !ele.skip && ele.ft.equals(FeatureType.categorical)).collect(Collectors.toList());
        Map<String, Double> corrData = categoricalToCategorical.apply(kpiCol, deps);
        for (Map.Entry<String, Double> entry : corrData.entrySet()) {
          dd.addToChiSqCorrelation(entry.getKey(), entry.getValue());
        }
        // get all the continous cols
        List<ColumnData> contiCols = columnMappings.values().stream().filter(ele -> !ele.name.equalsIgnoreCase(kpi)
                && !ele.possiblePrimaryKey && !ele.skip && ele.ft.equals(FeatureType.continuous)).collect(Collectors.toList());
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
              double corr = catToCont.apply(kpiCol, contiCol, Double.NaN, Double.NaN);
              dd.addToAnova(contiCol.name, corr);
            });
          }
        }

      }
      return dd;
    } catch(SQLException sqle) {
      throw new RuntimeException((sqle));
    }
  };


  private Function<String, DataTable> deepInsightGraphComputer = graphKey -> {
    String featureCol = graphKey.substring(0, graphKey.indexOf("||"));
    String metricCol = graphKey.substring(graphKey.indexOf("||") + "||".length());
    return getDeepInsightGraphResponse(featureCol, metricCol, ingesterThreadLocal.get());
  };

  DataTable getDeepInsightGraphResponse(String feature, String metric, SQLIngester ingester) {
    DataTable response = null ;
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

  private static DataTable getResponseForDeepInsights(String tableName, String featureCol, int actualFeatureColType,
                                                      String metricCol,
                                                      String unbinnedMetricCol, SQLIngester ingester,
                                                      TableData.ColumnData metricColCd)  {
     try{
    if (!metricColCd.skip && metricColCd.ft.equals(FeatureType.continuous)) {
      String query = String.format(QUERY_DEEP_METRIC_CONTI, unbinnedMetricCol, featureCol, tableName);
      ResultSet rs = ingester.executeQuery(query);
      int featureColType = rs.getMetaData().getColumnType(3);
      List<TableRow> dataPoints = new LinkedList<>();
      final boolean isMetricNumeric = true ;
      final boolean isFeatureNumeric ;
      final boolean isFeatureRange;
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
        TableRow tr = new TableRow();

        Value v = null;
        if (featureColType == Types.VARCHAR) {
          v = new TextValue(key.toString());
        } else {
          v = new NumberValue(Double.parseDouble(key));
        }
        TableCell tc = new TableCell(v, key);
        tc.setCustomProperty("numElements", String.valueOf(count));
        if (lb != null) {
          tc.setCustomProperty("featureLowerBound", lb);
        }
        if (ub != null) {
          tc.setCustomProperty("featureUpperBound", ub);
        }
        tr.addCell(tc);
        tr.addCell(avg);
        dataPoints.add(tr);
      }
      numRows = dataPoints.size();
      if (isFeatureNumeric || isMetricNumeric) {
        dataPoints = dataPoints.stream().sorted((tr1, tr2) -> {
          if (isFeatureNumeric) {
            if (isFeatureRange) {
              double db1 =  Double.parseDouble(tr1.getCell(0).getCustomProperty("featureLowerBound"));
              double db2 = Double.parseDouble(tr2.getCell(0).getCustomProperty("featureLowerBound"));
              if (db1 == db2) {
                return 0;
              } else {
                return  db1 < db2 ? -1: 1;
              }
            } else {
              double db1 =  Double.parseDouble(tr1.getCell(0).getFormattedValue());
              double db2 = Double.parseDouble(tr2.getCell(0).getFormattedValue());
              if (db1 == db2) {
                return 0;
              } else {
                return  db1 < db2 ? -1: 1;
              }

            }
          } else {
            double db1 =  ((NumberValue)tr1.getCell(1).getValue()).getValue();
            double db2 = ((NumberValue)tr2.getCell(1).getValue()).getValue();
            if (db1 == db2) {
              return 0;
            } else {
              return  db1 < db2 ? -1: 1;
            }
          }
        }).collect(Collectors.toList());
      }
      final DataTable dt = new DataTable();

      ColumnDescription metricDesc = new ColumnDescription("metric",
              isMetricNumeric ? ValueType.NUMBER : ValueType.TEXT, metricCol);
      ColumnDescription featureDesc = new ColumnDescription("feature",
              (isFeatureNumeric && !isFeatureRange) ? ValueType.NUMBER : ValueType.TEXT, featureCol);
      featureDesc.setCustomProperty("isFeatureRange", Boolean.toString(isFeatureRange));
      dt.addColumn(featureDesc);
      dt.addColumn(metricDesc);

      dt.addRows(dataPoints);
      dt.setCustomProperty("graphType", numRows > 50 ? AREA_CHART :  BAR_CHART/* isFeatureRange ? HISTOGRAM : BAR_CHART*/);
      return dt;
    } else if (!metricColCd.skip && metricColCd.ft.equals(FeatureType.categorical)) {
      String query = String.format(QUERY_DEEP_METRIC_CAT, featureCol, metricCol, tableName);
      ResultSet rs = ingester.executeQuery(query);
      final int featureColType = rs.getMetaData().getColumnType(2);
      int metricColType = rs.getMetaData().getColumnType(3);
      Map<String, TableRow> keyToRow = new HashMap<>();
      final Map<String, Integer > kpiValueToIndex = new HashMap<>();
      final boolean isMetricNumeric;
      final boolean isFeatureNumeric;
      final boolean isFeatureRange;
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
        final String key ;
        final String lb ;
        final String ub;

        if (featureColType == Types.VARCHAR) {
          key = rs.getString(2);
          if (isFeatureNumeric && isFeatureRange) {
            lb = key.substring(0, key.indexOf(" - ")).trim();
            ub = key.substring(key.indexOf(" - ")).trim();
          } else {
            ub = null;
            lb = null;
          }
        } else {
          key = rs.getObject(2).toString();
          ub = null;
          lb = null;
        }

        String kpi = rs.getObject(3).toString();
        TableRow tr = keyToRow.computeIfAbsent(key, k -> {
          Value v = null;
          if (featureColType == Types.VARCHAR) {
            v = new TextValue(k);
          } else {
            v = new NumberValue(Double.parseDouble(k));
          }
          TableCell tc = new TableCell(v, k);
          //tc.setCustomProperty("numElements", String.valueOf(count));
          if (lb != null) {
            tc.setCustomProperty("featureLowerBound", lb);
          }
          if (ub != null) {
            tc.setCustomProperty("featureUpperBound", ub);
          }
          TableRow trr = new TableRow();
          trr.addCell(tc);
          return trr;
        });
        //add nulls if needed
        int indexInRow = kpiValueToIndex.computeIfAbsent(kpi, k -> {
          return kpiValueToIndex.size() + 1;
        });
        int len = tr.getCells().size();
        for(int i = 0; i < indexInRow - len + 1; ++i) {
          tr.addCell(new MutableWrapperTableCell());
        }
        TableCell metricCell = new TableCell(new NumberValue(count));
        metricCell.setCustomProperty("metricValue", kpi);
        MutableWrapperTableCell mwtc = (MutableWrapperTableCell) tr.getCell(indexInRow);
        mwtc.setMutableCell(metricCell);
      }
      numRows = keyToRow.size();
      List<TableRow> dataPoints = keyToRow.values().stream().sorted((tr1, tr2) -> {
        if (isFeatureNumeric) {
          if (isFeatureRange) {
            double db1 = Double.parseDouble(tr1.getCell(0).getCustomProperty("featureLowerBound"));
            double db2 = Double.parseDouble(tr2.getCell(0).getCustomProperty("featureLowerBound")) ;
            if (db1 == db2) {
              return 0;
            } else {
              return  db1 < db2 ? -1: 1;
            }

          } else {
            double db1 = Double.parseDouble(tr1.getCell(0).getFormattedValue());
            double db2 = Double.parseDouble(tr2.getCell(0).getFormattedValue());
            if (db1 == db2) {
              return 0;
            } else {
              return  db1 < db2 ? -1: 1;
            }
          }
        } else {
          double db1 = ((NumberValue) tr1.getCell(1).getValue()).getValue();
          double db2 = ((NumberValue) tr2.getCell(1).getValue()).getValue();
          if (db1 == db2) {
            return 0;
          } else {
            return  db1 < db2 ? -1: 1;
          }
        }
        }).collect(Collectors.toList());
      final DataTable dt = new DataTable();


      ColumnDescription featureDesc = new ColumnDescription("feature",
              (isFeatureNumeric && !isFeatureRange) ? ValueType.NUMBER : ValueType.TEXT, featureCol);
      featureDesc.setCustomProperty("isFeatureRange", Boolean.toString(isFeatureRange));
      dt.addColumn(featureDesc);
      List<String> yCols = kpiValueToIndex.entrySet().stream().sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue())).
              map(e -> e.getKey()).collect(Collectors.toList());
      yCols.forEach(kname -> {
        ColumnDescription metricDesc = new ColumnDescription(kname,
                ValueType.NUMBER, kname);
        dt.addColumn(metricDesc);
      });

      dt.addRows(dataPoints);
      dt.setCustomProperty("graphType", numRows > 50 ? AREA_CHART :  BAR_CHART/*isFeatureRange ? HISTOGRAM : BAR_CHART*/);
      return dt;
    }
    return null;
  } catch (Exception sqle) {
     throw new RuntimeException(sqle);
  }
  }

  TableData(String tableOrQuery, SQLIngester ingester, int workFlowId, boolean isQuery, Set<String> joinCols) throws SQLException, IOException {
    this(tableOrQuery, ingester, workFlowId, isQuery,false, joinCols, Collections.EMPTY_SET);
  }
  TableData(String tableOrQuery, SQLIngester ingester, int workFlowId, boolean isQuery,
      boolean isShadowTable, Set<String> joinCols, Set<String> possiblePrimaryKeys)
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
      if (!possiblePrimaryKeys.isEmpty()) {
        for(ColumnData cd: cds) {
          cd.possiblePrimaryKey = possiblePrimaryKeys.contains(cd.name);
        }
      }
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
            numBuckets = range/4;
          } else if (range < 1000) {
            numBuckets = range/6;
          } else {
            numBuckets = range /8 ;
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
            true, TableData.this.joinCols,
                TableData.this.columnMappings.entrySet().stream().
                        filter(entry -> entry.getValue().possiblePrimaryKey).
                        map(entry -> entry.getKey()).collect(Collectors.toSet()));

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

  public DataTable getDeepInsightGraphData(String metricCol, String featureCol,
                                               SQLIngester ingester) throws Exception {
    ingesterThreadLocal.set(ingester);
    String graphKey = featureCol + "||" + metricCol;
    return deepInsightGraphData.computeIfAbsent(graphKey, deepInsightGraphComputer );
  }

  public DataTable getDeepInsightGraphData(String metricCol, String featureCol,
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

  private static class MutableWrapperTableCell extends TableCell {
    volatile  private TableCell mutableCell;
    MutableWrapperTableCell() {
      super(new NumberValue(0));
      mutableCell = null;
    }

    @Override
    public String getFormattedValue() {
      return mutableCell != null ? mutableCell.getFormattedValue(): super.getFormattedValue();
    }

    @Override
    public void setFormattedValue(String formattedValue) {
      if (mutableCell != null) {
        mutableCell.setFormattedValue(formattedValue);
      } else {
        super.setFormattedValue(formattedValue);
      }
    }

    @Override
    public Value getValue() {
      return mutableCell != null ? mutableCell.getValue(): super.getValue();
    }

    @Override
    public ValueType getType() {
      return mutableCell != null ? mutableCell.getType(): super.getType();
    }

    @Override
    public boolean isNull() {
      return mutableCell != null ? mutableCell.isNull(): super.isNull();
    }

    @Override
    public String toString() {
      return mutableCell != null ? mutableCell.toString(): super.toString();
    }

    @Override
    public String getCustomProperty(String key) {
      return mutableCell != null ? mutableCell.getCustomProperty(key): super.getCustomProperty(key);
    }

    @Override
    public void setCustomProperty(String propertyKey, String propertyValue) {
      if (mutableCell != null) {
        mutableCell.setCustomProperty(propertyKey, propertyValue);
      } else {
        super.setCustomProperty(propertyKey, propertyValue);
      }
    }

    @Override
    public Map<String, String> getCustomProperties() {
      return mutableCell != null ? mutableCell.getCustomProperties(): super.getCustomProperties();
    }

    @Override
    public TableCell clone() {
      return mutableCell != null ? mutableCell.clone(): super.clone();
    }

    void setMutableCell(TableCell cell) {
      this.mutableCell = cell;
    }
  }

  @FunctionalInterface
  static interface QuadFunction<A,B,C,D,R> {

    R apply(A a, B b, C c, D d);

    default <V> QuadFunction<A, B, C, D, V> andThen(
            Function<? super R, ? extends V> after) {
      Objects.requireNonNull(after);
      return (A a, B b, C c, D d) -> after.apply(apply(a, b, c, d));
    }
  }

}
