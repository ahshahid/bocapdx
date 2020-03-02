package io.boca.internal.tables;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;

public class TableData {
  private long totalRows;
  private final String tableName;
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
      + "val tableDf = snappysession.table(\"%1$s\");"
      + "val tableSchema = tableDf.schema;"
      + "val stringIndexer_suffix = \"_boca_index\";"
      + "val depsColIndxs = Array[Int](%2$s);"
      + "val kpiColsIndex = %3$s;"
      + "// split the cols indexes into string & numerical for now"
      + "val (depStringCols, depNonStringCols) = depsColIndxs.partition(i => tableSchema(i).dataType.equals(StringType));"
      + "// create string indexers for string type dependent cols"
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
      + "                    } else preparedDf_1 -> None"
      + "import org.apache.spark.ml.feature.VectorAssembler;"
      + "import org.apache.spark.ml.linalg.Vectors;"
      + "val featureArray = depNonStringCols.map(i => tableSchema(i).name) ++ indexcolsname;"
      + "val assembler = new VectorAssembler().setInputCols(featureArray)).setOutputCol(\"features\");"
      + "val dfWithFeature = assembler.transform(preparedDf);"
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

  private static String contiToContiCorr = "import org.apache.spark.mllib.linalg._;"
      + "import org.apache.spark.sql._;"
      + "import org.apache.spark.sql.types._;"
      + "import org.apache.spark.mllib.stat._;"
      + "val tableDf = snappysession.table(\"%1$s\");"
      + "val inputVectorRDD = tableDf.rdd.map[Vector](row => {"
      + "val arr = Array(row(%2$s),row(%3$s));"
      + "val doubleArr = arr.map(elem => {"
      +  " elem match {"
      +     "case x: Short => x.toDouble;"
      +     "case x: Int => x.toDouble;"
      +     "case x: Long => x.toDouble;"
      +     "case x: Float => x.toDouble;"
      +     "case x: Double => x;"
      +     "case x: Decimal => x.toDouble;"
      +     "case _ => throw new RuntimeException(\"unknown type\");"
      +     "}"
      +   "});"
      +   "Vectors.dense(doubleArr);"
      +  "});"
      //+ "val elementType1 = new ObjectType(classOf[Vector]);"
      //+ "val inputDataFrame = snappysession.createDataFrame(inputRowRDD, StructType(Seq(StructField(\"feature\", elementType1, false))));"
      + "val matrix = Statistics.corr(inputVectorRDD, \"pearson\");"
      + "val corrValue = matrix(0,0);"
      + "val dfStrct = StructType(Seq(StructField(\"corr\", DoubleType, false)));"
      + "val valueDf = snappysession.sqlContext.createDataFrame(java.util.Collections.singletonList(Row(corrValue)), dfStrct);";

  //+ "val valueDf = snappysession.sqlContext.createDataFrame(java.util.Collections.singletonList(Row(corrValue)), dfStrct)";
     // + "val rsDf = org.apache.spark.ml.stat.Correlation.corr(inputDataFrame, \"feature\" );"
     // + "val valueDf = rsDf.map[Row](row => Row(row(0).asInstanceOf[Matrix](0,0)))(Encoders.Double);";

  private static ThreadLocal<SQLIngester> ingesterThreadLocal = new ThreadLocal<SQLIngester>();

  private BiFunction<ColumnData, ColumnData, Double> kpiContToCont = (kpiCd, depCd) -> {
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

    String scalaCodeToExecute = String.format(contiToContiCorr,
        this.getTableName(), depColIndex, kpiColIndex);
    SQLIngester ingester = ingesterThreadLocal.get();
    try {
      ResultSet rs = ingester.executeQuery("exec scala options (returnDF 'valueDf') " + scalaCodeToExecute);
      rs.next();
      return rs.getDouble(1);
    } catch(SQLException sqle) {
      throw new RuntimeException(sqle);
    }

  };

  private BiFunction<ColumnData, List<ColumnData>, Map<Integer,Double>> categoricalToCategorical = (kpiCd, depCds) -> {
    List<Schema.SchemaColumn> cols = getSchema().getColumns();

    int kpiColIndex = -1;
    List<Integer> depColsIndex = new ArrayList<>();

    Stream<ColumnData> stream = depCds.stream();
    for(int i = 0 ; i < cols.size(); ++i) {
      Schema.SchemaColumn sc = cols.get(i);
      if (kpiColIndex == -1) {
        if (sc.getName().equalsIgnoreCase(kpiCd.name)) {
          kpiColIndex = i;
        } else if (stream.anyMatch(elem -> elem.name.equalsIgnoreCase(sc.getName()))) {
          depColsIndex.add(i);
        }
      } else  if (stream.anyMatch(elem -> elem.name.equalsIgnoreCase(sc.getName()))) {
          depColsIndex.add(i);
      }
    }
    StringBuilder sb = new StringBuilder();
    depColsIndex.stream().forEach(i -> sb.append(i).append(','));
    sb.deleteCharAt(sb.length() -1);
    String depsColindxsStr = sb.toString();
    String scalaCodeToExecute = String.format(catToCatCorr,
        this.getTableName(), depsColindxsStr, kpiColIndex);
    SQLIngester ingester = ingesterThreadLocal.get();
    try {
      ResultSet rs = ingester.executeQuery("exec scala options (returnDF 'valueDf') " + scalaCodeToExecute);
      rs.next();
      return rs.getDouble(1);
    } catch(SQLException sqle) {
      throw new RuntimeException(sqle);
    }

  };

  private Function<String, DependencyData> depedencyComputer = kpi -> {
    DependencyData dd = new DependencyData();
    ColumnData kpiCol = columnMappings.get(kpi.toLowerCase());

    if (kpiCol.ft.equals(FeatureType.continuous)) {
     for(ColumnData cd: columnMappings.values()) {
       if (!cd.name.equalsIgnoreCase(kpi)) {
         if (cd.ft.equals(FeatureType.continuous)) {
           double corr = kpiContToCont.apply(kpiCol, cd);
           dd.add(cd.name, corr);
         }
       }
     }
    } else if (kpiCol.ft.equals(FeatureType.categorical)) {
      // get all the categorical cols
      List<ColumnData> deps = columnMappings.values().stream().filter(ele -> !ele.name.equalsIgnoreCase(kpi)
          && ele.ft.equals(FeatureType.categorical)).collect(Collectors.toList());
      categoricalToCategorical.apply(kpiCol, deps);
      /*for(ColumnData cd: columnMappings.values()) {
        if (!cd.name.equalsIgnoreCase(kpi)) {
          if (cd.ft.equals(FeatureType.categorical)) {
            double corr = categoricalToCategorical.apply(kpiCol, cd);
            dd.add(cd.name, corr);
          }
        }
      }*/
    }
    return dd;
  };


  TableData(String tableName, SQLIngester ingester) throws SQLException {
    // filter columns of interest.
    // figure out if they are continuous or categorical
    this.tableName = tableName;
    this.schema = ingester.getSchema("select * from " + tableName);
    // get total rows
    ResultSet rs = ingester.executeQuery("select count(*) from " + tableName);
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
  }

  private Schema getSchema() {
    return this.schema;
  }

  private String getTableName() {
    return this.tableName;
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
            -> str1 + "," + " count(distinct " + str2 + ")").substring(1);
        String query = "select  %1$s from %2$s";
        ResultSet rs = ingester.executeQuery(String.format(query, countClause, this.tableName));
        rs.next();
        int i = 1;
        for (Schema.SchemaColumn sc : scs) {
          long num = rs.getLong(i);
          boolean skip = false;
          FeatureType ft = null;
          int percent = (int)((100 * num) / totalRows);
          if (percent > 80) {
            skip = true;
          } else if (percent < 10) {
            ft = FeatureType.categorical;
            skip = false;
          } else {
            skip = false;
            ft = FeatureType.continuous;
          }

          cds[i - 1] = new ColumnData(sc.getName().toLowerCase(), ft, skip, sqlType);
          ++i;
        }
        return cds;
      }
      case Types.DECIMAL:
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE: {
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName().toLowerCase(), FeatureType.continuous, false,
              sqlType);
        }
        return cds;
      }
      case Types.NCHAR:
      case Types.CHAR:{
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName().toLowerCase(), FeatureType.categorical, false,
              sqlType);
        }
        return cds;
      }
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
      {
        String countClause = scs.stream().map(sc -> sc.getName().toLowerCase()).reduce("", (str1, str2)
            -> str1 + "," + " count(distinct " + str2 + ")").substring(1);
        String query = "select  %1$s from %2$s";
        ResultSet rs = ingester.executeQuery(String.format(query, countClause, this.tableName));
        rs.next();
        int i = 1;
        for (Schema.SchemaColumn sc : scs) {
          long num = rs.getLong(i);
          boolean skip = false;
          FeatureType ft = null;
          int percent = (int)((100 * num) / totalRows);
          if (percent > 80) {
            skip = true;
          } else {
            ft = FeatureType.categorical;
            skip = false;
          }
          cds[i - 1] = new ColumnData(sc.getName().toLowerCase(), ft, skip, sqlType);
          ++i;
        }
        return cds;
      }
      default: {
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName().toLowerCase(), null, true, sqlType);
        }
        return cds;
      }

    }
  }

  private static class ColumnData {
    FeatureType ft;
    boolean skip;
    int sqlType;
    String name;

    ColumnData(String name, FeatureType ft, boolean skip, int sqlType) {
      this.ft = ft;
      this.skip = skip;
      this.sqlType = sqlType;
      this.name = name;
    }
  }

}

enum FeatureType {
 categorical, continuous, date
}