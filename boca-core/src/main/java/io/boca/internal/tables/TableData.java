package io.boca.internal.tables;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;

public class TableData {
  private long totalRows;
  private final int workFlowId;
  public final boolean isQuery;
  private final List<List<String>> sampleRows;
  private static final double pValueThreshold = 0.1d;
  private final String tableOrQuery;
  private final String tableOrView;
  private final Schema schema;
  private Map<Integer, ColumnData[]> sqlTypeToColumnMappings = new HashMap<>();
  private Map<String, ColumnData> columnMappings = new HashMap<>();
  private ConcurrentHashMap<String, DependencyData >kpiDependencyMap =
      new ConcurrentHashMap<>();

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
      + "val features = chiSqTestResult.filter { case (res, _) => res.pValue < pValueThreshold};"
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
      + "val depColIndexes = %2$s;"
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


  private static ThreadLocal<SQLIngester> ingesterThreadLocal = new ThreadLocal<SQLIngester>();

  private BiFunction<ColumnData, List<ColumnData> , double[]> kpiContToCont = (kpiCd, depCds) -> {
    StringBuilder depColIndexes = new StringBuilder("Array[Int](");
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
    depColIndexes.deleteCharAt(depColIndexes.length() - 1).append(')');


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
      if (kpiCol.numDistinctValues == 2) {
        if (kpiCol.sqlType == Types.SMALLINT ||
            kpiCol.sqlType == Types.INTEGER || kpiCol.sqlType == Types.BIGINT || kpiCol.sqlType == Types.DOUBLE
            || kpiCol.sqlType == Types.FLOAT) {
          double[] corrs = kpiContToCont.apply(kpiCol, contiCols);
          for(int i = 0 ; i < corrs.length; ++i) {
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
    return dd;
  };


  TableData(String tableOrQuery, SQLIngester ingester, int workFlowId, boolean isQuery) throws SQLException, IOException {
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
    // get total rows
    String countQuery = "select count (*) from " + tableOrView;
    ResultSet rs = ingester.executeQuery(countQuery);
    rs.next();
    totalRows = rs.getLong(1);
    // segregate columns based on sql types
    Map<Integer, List<Schema.SchemaColumn>> groups =  schema.getColumns().stream().
        collect(Collectors.groupingBy(sc -> sc.sqlType()));

    for(Map.Entry<Integer, List<Schema.SchemaColumn>> entry : groups.entrySet()) {
       ColumnData[] cds = determineColumnData(entry.getKey(), entry.getValue(),
           ingester);
      sqlTypeToColumnMappings.put(entry.getKey(), cds);
      for(ColumnData cd: cds) {
        columnMappings.put(cd.name.toLowerCase(), cd);
      }
    }

    // check if the table contains int / big int/ doube/float columns which are continuous & not primary key ones.
    // if yes we may have to create a shadow prepped table which is binned.
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
              ft = FeatureType.continuous;
            } else if (percent < 10) {
              ft = FeatureType.categorical;
              skip = false;
            } else {
              skip = false;
              ft = FeatureType.continuous;
            }
          } else {
            skip = true;
          }
          // if approx distinct values is > 90 % of total rows , get exact figure
          if (distinctValues > (90 * totalRows)/100) {
            if (!isQuery) {
              possiblePkIndexes.add(k);
            }
          }
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
              ft = FeatureType.continuous;
            } else if (percent < 10) {
              ft = FeatureType.categorical;
              skip = false;
            } else {
              skip = false;
              ft = FeatureType.continuous;
            }
          } else {
            skip = true;
          }

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
