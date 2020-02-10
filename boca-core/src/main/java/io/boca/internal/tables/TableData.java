package io.boca.internal.tables;

import java.util.concurrent.ConcurrentHashMap;

import macrobase.ingest.SQLIngester;

public class TableData {

  TableData(String tableName, SQLIngester ingester) {

  }
  private ConcurrentHashMap<String, DependencyData >kpiDependencyMap =
      new ConcurrentHashMap<>();

  public DependencyData getDependencyData(String kpiColumn) {
    return null;
  }

}
