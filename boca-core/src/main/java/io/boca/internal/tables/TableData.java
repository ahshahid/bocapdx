package io.boca.internal.tables;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.*;
import java.util.stream.Collectors;

import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;

public class TableData {
  private long totalRows;
  private final String tableName;
  private Map<Integer, ColumnData[]> columnMappings = new HashMap<>();
  TableData(String tableName, SQLIngester ingester) throws SQLException {
    // filter columns of interest.
    // figure out if they are continuous or categorical
    this.tableName = tableName;
    Schema schema = ingester.getSchema("select * from " + tableName);
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
       columnMappings.put(entry.getKey(), cds);
    }
  }
  private ConcurrentHashMap<String, DependencyData >kpiDependencyMap =
      new ConcurrentHashMap<>();

  public DependencyData getDependencyData(String kpiColumn) {
    return null;
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
      case Types.NCHAR:
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