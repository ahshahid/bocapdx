package io.boca.internal.tables;

import java.util.HashMap;
import java.util.Map;

public class DependencyData {
  private Map<String, Double> correlationMapping = new HashMap<>();

  public void add(String depCol, double corr) {
    this.correlationMapping.put(depCol, corr);
  }

}
