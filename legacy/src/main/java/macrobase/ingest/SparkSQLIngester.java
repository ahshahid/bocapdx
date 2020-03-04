package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;

import java.sql.SQLException;

public class SparkSQLIngester extends SQLIngester{

    static {
        //load the driver class so that shaded jar ibcludes it
         io.snappydata.jdbc.ClientDriver.class.getName();
    }
    public SparkSQLIngester(MacroBaseConf conf) throws ConfigurationException, SQLException {
        super(conf);
    }

    @Override
    public String getDriverClass() {
        return "io.snappydata.jdbc.ClientDriver";
    }

    @Override
    public String getJDBCUrlPrefix() {
        return "jdbc:snappydata:";
    }
}
