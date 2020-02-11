package io.boca.internal.tables;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import macrobase.ingest.SQLIngester;

public class TableManager {
  private static TableManager singleton = new TableManager();

  private ConcurrentHashMap<String, TableData> tablesMap = new ConcurrentHashMap<>();

  private TableManager() {

  }

  public static TableData getTableData(String fqTableName, SQLIngester ingester) {
    return singleton.tablesMap.computeIfAbsent(fqTableName, (key) -> {try {
      return new TableData(key, ingester);
    } catch (SQLException sqle) {
       throw new RuntimeException(sqle);
    }});
  }

}
