package macrobase.runtime.resources;

import macrobase.conf.MacroBaseConf;
import macrobase.ingest.SQLIngester;
import macrobase.ingest.result.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Path("/sampleRows")
@Produces(MediaType.APPLICATION_JSON)

public class SampleRowsResource extends BaseResource {
  private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

  @Context
  private HttpServletRequest request;



  static class SampleRowsResponse {
    public List<List<String>> rows = new ArrayList<>(100);
    public String errorMessage;
  }

  public SampleRowsResource(MacroBaseConf conf) {
    super(conf);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public SampleRowsResponse getSample(SchemaResource.SchemaRequest schmaReq) {
    SampleRowsResponse response = new SampleRowsResponse();
    HttpSession ss = request.getSession();
    try {
      SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);
      ResultSet rs = ingester.executeQuery("select * from "
          + schmaReq.tablename + " LIMIT 100");
      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();

      while(rs.next()) {
        List<String> row = new ArrayList<>();
        for (int i = 1; i <= numCols; ++i) {

          switch (rsmd.getColumnType(i)) {
            case Types.ARRAY : {
              Array temp = rs.getArray(i);
              if (temp != null) {
                Object [] data = (Object[])temp.getArray();
                StringBuilder sb = new StringBuilder();
                for(Object ele: data) {
                  sb.append(ele).append(", ");
                }
                sb.deleteCharAt(sb.length() -1);
                row.add(sb.toString());
              } else {
                row.add(null);
              }
              break;
            }
            case Types.ROWID:
            case Types.BIGINT : {
              long temp = rs.getLong(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }

            case Types.BIT : {
              byte temp = rs.getByte(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.BLOB : {
              byte[] temp = rs.getBytes(i);
              if (!rs.wasNull()) {
                row.add("blob data");
              } else {
                row.add(null);
              }
              break;
            }
            case Types.BOOLEAN : {
              boolean temp = rs.getBoolean(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.NCLOB :
            {
              NClob temp = rs.getNClob(i);
              if (!rs.wasNull()) {
                long length = temp.length();
                int intLength = length > Integer.MAX_VALUE? Integer.MAX_VALUE: (int)length;

                char [] buff = new char[intLength];

                Reader reader = temp.getCharacterStream();
                int numRead = 0;
                int totalRead = 0;
                while((numRead = reader.read(buff, totalRead,
                    intLength - totalRead)) != -1) {
                  totalRead += numRead;
                }
                row.add(new String(buff));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.CLOB : {
              Clob temp = rs.getClob(i);
              if (!rs.wasNull()) {
                long length = temp.length();
                int intLength = length > Integer.MAX_VALUE? Integer.MAX_VALUE: (int)length;

                char [] buff = new char[intLength];

                Reader reader = temp.getCharacterStream();
                int numRead = 0;
                int totalRead = 0;
                while((numRead = reader.read(buff, totalRead,
                    intLength - totalRead)) != -1) {
                  totalRead += numRead;
                }
                row.add(new String(buff));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.DATE : {
              Date temp = rs.getDate(i);
              if (!rs.wasNull()) {
                row.add(temp.toString());
              } else {
                row.add(null);
              }
              break;
            }
            case Types.DECIMAL : {
              BigDecimal temp = rs.getBigDecimal(i);
              if (!rs.wasNull()) {
                row.add(temp.toString());
              } else {
                row.add(null);
              }
              break;
            }
            case Types.DOUBLE : {
              double temp = rs.getDouble(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.REAL:
            case Types.FLOAT : {
              float temp = rs.getFloat(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.INTEGER : {
              int temp = rs.getInt(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.SMALLINT : {
              short temp = rs.getShort(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.JAVA_OBJECT : {
              Object temp = rs.getObject(i);
              if (!rs.wasNull()) {
                row.add(temp.toString());
              } else {
                row.add(null);
              }
              break;
            }
            case Types.LONGNVARCHAR :
            case Types.NCHAR :
            case Types.NVARCHAR :
              {
              String temp = rs.getNString(i);
              if (!rs.wasNull()) {
                row.add(temp);
              } else {
                row.add(null);
              }
              break;
            }

            case Types.LONGVARBINARY :
            case Types.LONGVARCHAR :
            case Types.CHAR :
            case Types.VARCHAR :
            case Types.BINARY :
              {
              String temp = rs.getString(i);
              if (!rs.wasNull()) {
                row.add(temp);
              } else {
                row.add(null);
              }
                break;
            }
            case Types.NUMERIC:
            {
              float temp = rs.getFloat(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            {
              Timestamp temp = rs.getTimestamp(i);
              if (!rs.wasNull()) {
                row.add(temp.toString());
              } else {
                row.add(null);
              }
              break;
            }
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            {
              Time temp = rs.getTime(i);
              if (!rs.wasNull()) {
                row.add(temp.toString());
              } else {
                row.add(null);
              }
              break;
            }
            case Types.TINYINT : {
              byte temp = rs.getByte(i);
              if (!rs.wasNull()) {
                row.add(String.valueOf(temp));
              } else {
                row.add(null);
              }
              break;
            }
            default: {
              throw new RuntimeException("Unhandled sql type = " + rsmd.getColumnType(i));
            }

          }

        }
        response.rows.add(row);
      }

    } catch (Exception e) {
      log.error("An error occurred while processing a request: {}", e);
      response.errorMessage = ExceptionUtils.getStackTrace(e);
    }



    return response;
  }


}
