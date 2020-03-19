package io.boca.internal.tables;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import macrobase.ingest.SQLIngester;

public class TableManager {
  private static TableManager singleton = new TableManager();
  private AtomicInteger uid = new AtomicInteger();
  private ConcurrentHashMap<String, TableData> tablesMap = new ConcurrentHashMap<>();
  private HashMap<Integer, String> workFlowToTableDef = new HashMap<>();

  private TableManager() {

  }

  public static TableData getTableData(String fqTableNameOrQuery, SQLIngester ingester, boolean isQuery,
      Set<String> joinCols) {
    return singleton.tablesMap.computeIfAbsent(fqTableNameOrQuery, (key) -> {try {
      int uid = singleton.uid.incrementAndGet();
      TableData td = new TableData(key, ingester, uid, isQuery, joinCols);
      singleton.workFlowToTableDef.put(uid, key);
      return td;
    } catch (Exception e) {
       throw new RuntimeException(e);
    }});
  }

  public static TableData getTableData(int workFlowId) {
    String tableDef = singleton.workFlowToTableDef.get(workFlowId);
    if (tableDef != null) {
      return getTableData(tableDef, null, false, Collections.emptySet());
    } else {
      throw new RuntimeException("Table Def for work flow id not found");
    }

  }

}
