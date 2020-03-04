package macrobase.ingest.result;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Schema {
    public static class SchemaColumn {
        private String name;
        private String type;
        private int sqlType;

        public SchemaColumn(String name, String type, int sqlType) {
            this.name = name;
            this.type = type;
            this.sqlType = sqlType;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public String getType() {
            return type;
        }

        public SchemaColumn() {
            // Jackson
        }

        public int sqlType() {
          return this.sqlType;
        }
    }

    private List<SchemaColumn> columns;

    @JsonProperty
    public List<SchemaColumn> getColumns() {
        return columns;
    }

    public Schema(List<SchemaColumn> _columns) {
        columns = _columns;
    }

    public Schema() {
        // Jackson
    }
}
