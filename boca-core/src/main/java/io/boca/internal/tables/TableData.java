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

  private static String contiToContiCorr = "val tableDf = snappysession.table(%1s);"
      + "import org.apache.spark.ml.linalg._;"
      + "implicit val vectorEncoder = org.apache.spark.sql.Encoders.kryo[DenseVector]"
      + "val inputDataSet = tableDf.map[Vector](row => {"
      + "val arr = Array(row(%2s),row(%3s));"
      + "val doubleArr = arr.map[Double](elem => {"
      +  " elem match {"
      +     "case x: Short => x.toDouble;"
      +     "case x: Int => x.toDouble;"
      +     "case x: Long => x.toDouble;"
      +     "case x: Float => x.toDouble;"
      +     "case x: Double => x;"
      +     "case x: Decimal => x.toDouble;"
      +     "}"
      +   "})"
      +   "Vectors.dense(doubleArr);"
      +  "});"
      +  "val rsDf = Correlation.corr(inputDataSet,inputDataSet.schema(0).name )"
      +  "val valueDf = rsDf.map[Row](row => Row(row(0).asInstanceOf[Matrix](0,0)))";

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

  private Function<String, DependencyData> depedencyComputer = kpi -> {
    ColumnData kpiCol = columnMappings.get(kpi);

    if (kpiCol.ft.equals(FeatureType.continuous)) {
     for(ColumnData cd: columnMappings.values()) {
       if (!cd.name.equals(kpi)) {
         if (cd.ft.equals(FeatureType.continuous)) {
           kpiContToCont.apply(kpiCol, cd);
         }
       }
     }
    }
    return null;
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
        columnMappings.put(cd.name, cd);
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
          cds[i - 1] = new ColumnData(sc.getName(), ft, skip, sqlType);
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
          cds[i++] = new ColumnData(sc.getName(), FeatureType.continuous, false,
              sqlType);
        }
        return cds;
      }
      case Types.NCHAR:
      case Types.CHAR:{
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName(), FeatureType.categorical, false,
              sqlType);
        }
        return cds;
      }
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
      {
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
          } else {
            ft = FeatureType.categorical;
            skip = false;
          }
          cds[i - 1] = new ColumnData(sc.getName(), ft, skip, sqlType);
          ++i;
        }
        return cds;
      }
      default: {
        int i = 0;
        for (Schema.SchemaColumn sc : scs) {
          cds[i++] = new ColumnData(sc.getName(), null, true, sqlType);
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