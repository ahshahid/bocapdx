package io.boca.internal.tables;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import macrobase.ingest.SQLIngester;

public class TableManager {
  private static TableManager singleton = new TableManager();
  private AtomicLong uid = new AtomicLong();
  private ConcurrentHashMap<String, TableData> tablesMap = new ConcurrentHashMap<>();
  private HashMap<Long, String> workFlowToTableDef = new HashMap<>();

  private TableManager() {

  }

  public static TableData getTableData(String fqTableNameOrQuery, SQLIngester ingester, boolean isQuery) {
    return singleton.tablesMap.computeIfAbsent(fqTableNameOrQuery, (key) -> {try {
      long uid = singleton.uid.incrementAndGet();
      TableData td = new TableData(key, ingester, uid, isQuery);
      singleton.workFlowToTableDef.put(uid, key);
      return td;
    } catch (Exception e) {
       throw new RuntimeException(e);
    }});
  }

  public static TableData getTableData(long workFlowId) {
    String tableDef = singleton.workFlowToTableDef.get(workFlowId);
    if (tableDef != null) {
      return getTableData(tableDef, null, false);
    } else {
      throw new RuntimeException("Table Def for work flow id not found");
    }

  }

}
