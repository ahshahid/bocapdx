package io.boca.internal.tables;

import java.util.HashMap;
import java.util.Map;

public class DependencyData {
  private Map<String, Double> pearsonCorrelationMapping = new HashMap<>();
  private Map<String, Double> chisqCorrelationMapping = new HashMap<>();

  public void addToPearson(String depCol, double corr) {
    this.pearsonCorrelationMapping.put(depCol, corr);
  }

  public void addToChiSqCorrelation(String depCol, double corr) {
    this.chisqCorrelationMapping.put(depCol, corr);
  }

}
