package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * First cut implementation with several shortcomings.
 * (1) Could take a while to load all the rows using single JDBC connection
 * (2) could OOM easily
 *
 */
public class JDBCDataFrameLoader implements DataFrameLoader {
    private final String tableName;
    private final String extraPredicate;
    private final String dburl;
    private Logger log = LoggerFactory.getLogger(JDBCDataFrameLoader.class);
    private final List<String> requiredColumns;
    private Map<String, Schema.ColType> columnTypes;
    // when reading file, convert nulls to String "NULL" (default should be true)
    private final boolean convertNulls;

    public JDBCDataFrameLoader(String url, String tableName, List<String> requiredColumns, String extraPredicate)
            throws IOException {
        this.requiredColumns = requiredColumns.stream().map(String::toLowerCase).collect(Collectors.toList());
        this.tableName = tableName;
        this.dburl = url;
        this.convertNulls = true;
        this.extraPredicate = extraPredicate;
    }
    
    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.columnTypes = types;
        return this;
    }

    public Schema getSchema(Connection connection)
            throws SQLException {

        Statement stmt = connection.createStatement();
        String colNames = String.join(",", requiredColumns);
        String sql = "SELECT " + colNames + " FROM " + tableName + " LIMIT 1" ;
        ResultSet rs = stmt.executeQuery(sql);

        Schema schema = new Schema();

        for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
            String colName = rs.getMetaData().getColumnName(i);
            Schema.ColType t = Schema.ColType.STRING;
            int type = rs.getMetaData().getColumnType(i);
            switch (type) {
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                    t = Schema.ColType.DOUBLE ;
            }
            schema.addColumn(t, colName.toLowerCase());
        }
        return  schema;
    }

    private Connection getConnection() throws SQLException {
        Connection connection;
        try {
            Class.forName("io.snappydata.jdbc.ClientDriver"); // fix later
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return DriverManager.getConnection(dburl);
    }

    private int getRowCount(Connection c) throws SQLException {
        int count = 0;
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from " + tableName);
        while (rs.next()) { count = rs.getInt(1); }
        if (count ==0 ) System.out.println("Table " + tableName + " has ZERO rows?? ");
        s.close();
        return count;
    }

    @Override
    public DataFrame load() throws Exception {
        DataFrame df = new DataFrame();

        Connection c = getConnection();
        Schema schema = getSchema(c);
        //int count = getRowCount(c);
        Statement stmt = c.createStatement();
        String colNames = String.join(",", requiredColumns);
        String sql = "SELECT " + colNames + " FROM " + tableName + " " + extraPredicate;
        System.out.println("REQUIRED COLUMNS = " + colNames);
        int sz = requiredColumns.size();
        ArrayList[] colValues = new ArrayList[sz];
        for (int i = 0; i < sz ; i++) colValues[i] = new ArrayList();

        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            for (int i = 0; i < sz; i++) {
                if (schema.getColumnTypeByName(requiredColumns.get(i)) == Schema.ColType.STRING) {
                    String s = rs.getString(i+1); // Hopefully type coercion works
                    if (s == null && convertNulls) {
                        s = "NULL";
                    }
                    colValues[i].add(s);
                } else {
                    colValues[i].add(rs.getDouble(i+1));
                }
            }
        }
        System.out.println("row count of cache " + colValues[0].size());
        // add all rows to DF
        for (int i = 0; i < sz ; i++) {
            if (schema.getColumnTypeByName(requiredColumns.get(i)) == Schema.ColType.STRING) {
                String[] values = new String[colValues[i].size()];
                values = ((List<String>) colValues[i]).toArray(values); // Hack. could OOM here.
                df.addColumn(requiredColumns.get(i), values);
            }
            else {
                double[] values = ((List<Double>) colValues[i]).stream().mapToDouble(d -> d).toArray();
                df.addColumn(requiredColumns.get(i), values);
            }
        }

        rs.close();
        df.prettyPrint();
        return df;
    }
}
