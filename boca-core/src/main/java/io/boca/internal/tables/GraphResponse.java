package io.boca.internal.tables;


import java.util.List;

public class GraphResponse {

    public String errorMessage;
    public String graphType;
    public List<GraphPoint> dataPoints;
    public boolean isMetricNumeric;
    public boolean isFeatureNumeric;
    public boolean isFeatureRange;

   public static class GraphPoint {
        public long numElements;
        public String feature;
        public String metric;

        public String featureLowerBound;
        public String featureUpperBound;

        public GraphPoint(long n, String f, String m, String flb,  String fub) {
            this.numElements = n;
            this.feature = f;
            this.metric = m;
            this.featureLowerBound = flb;
            this.featureUpperBound = fub;
        }
    }
}