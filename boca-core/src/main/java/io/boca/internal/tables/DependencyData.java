package io.boca.internal.tables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DependencyData {
  private Map<String, Double> pearsonCorrelationMapping = new HashMap<>();
  private Map<String, Double> chisqCorrelationMapping = new HashMap<>();
  private Map<String, Double> anovaMapping = new HashMap<>();
  private final FeatureType kpiType;
  private final String kpiColName;
  public DependencyData(String kpiColName, FeatureType kpiType) {
    this.kpiColName = kpiColName;
    this.kpiType = kpiType;
  }

  public void addToPearson(String depCol, double corr) {
    this.pearsonCorrelationMapping.put(depCol, corr);
  }

  public void addToAnova(String depCol, double corr) {
    this.anovaMapping.put(depCol, corr);
  }

  public void addToChiSqCorrelation(String depCol, double corr) {
    this.chisqCorrelationMapping.put(depCol, corr);
  }

  public Map<String, Double> getPearsonFeatureMap() {
    return Collections.unmodifiableMap(this.pearsonCorrelationMapping);
  }

  public Map<String, Double> getChisquareFeatureMap() {
    return Collections.unmodifiableMap(this.chisqCorrelationMapping);
  }

  public Map<String, Double> getAnovaFeatureMap() {
    return Collections.unmodifiableMap(this.anovaMapping);
  }

  public FeatureType getKpiColFeatureType() {
    return this.kpiType;
  }

}