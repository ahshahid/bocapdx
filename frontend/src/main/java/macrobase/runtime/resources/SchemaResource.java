package macrobase.runtime.resources;

import io.boca.internal.tables.TableData;
import io.boca.internal.tables.TableManager;
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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

@Path("/schema")
@Produces(MediaType.APPLICATION_JSON)

public class SchemaResource extends BaseResource {
    private static final Logger log = LoggerFactory.getLogger(SchemaResource.class);

    @Context
    private HttpServletRequest request;
    static class SchemaRequest {
        public Table table;
    }


    static class Table {
        public String name;
        public List<Join> joinlist;
        public void fillMissingPkColumns(SQLIngester ingester) {
          if (!(joinlist == null || joinlist.isEmpty())) {
              for(Join join: joinlist) {
                join.fillMissingPkColumn(this.name, ingester);
              }
          }
        }

        public boolean isQuery() {
          return !(this.joinlist == null || this.joinlist.isEmpty());
        }

        public void generateQuery(StringBuilder sb, boolean isTopNode) {
          if (isTopNode) {
              if (isQuery()) {
                  sb.append("select * from ").append(this.name).append(" ");
                  for(Join joinNode: this.joinlist) {
                      joinNode.generateQuery(sb, this);
                  }
              } else {
                  sb.append(this.name);
              }
          } else {
             if (isQuery()) {

                 for(Join joinNode: this.joinlist) {
                    joinNode.generateQuery(sb, this);
                 }
             }
          }
        }

    }

    static class Join {
        public String parentcol;
        public Table table;
        public String joincol;
        public String jointype;
        public void fillMissingPkColumn(String parentTableName, SQLIngester ingester) {
          if (this.jointype == null || this.jointype.trim().isEmpty()) {
              this.jointype = "inner join";
          }
          if (parentcol == null || parentcol.trim().isEmpty()) {
            TableData td = TableManager.getTableData(parentTableName.toLowerCase(), ingester, false);
            TableData childTd = TableManager.getTableData(table.name.toLowerCase(), ingester, false);
            if (joincol != null && !joincol.trim().isEmpty()) {
              TableData.ColumnData childCol = childTd.getColumnData(joincol.toLowerCase());
              this.parentcol = td.getFirstAvailablePkColumn(childCol.sqlType).name;
            } else {
              this.parentcol = td.getFirstAvailablePkColumn().name;
              this.joincol = childTd.getFirstAvailablePkColumn().name;
            }
          } else {
              TableData td = TableManager.getTableData(parentTableName.toLowerCase(), ingester, false);
              TableData childTd = TableManager.getTableData(table.name.toLowerCase(), ingester, false);
              if (joincol == null || joincol.trim().isEmpty()) {
                  TableData.ColumnData parentCol = td.getColumnData(parentcol.toLowerCase());
                  this.joincol = childTd.getFirstAvailablePkColumn(parentCol.sqlType).name;
              }
          }
          table.fillMissingPkColumns(ingester);
        }

        public void generateQuery(StringBuilder sb, Table parentTable) {
                sb.append(this.jointype).append(" ").append(this.table.name).
                append(" on ").append(parentTable.name).append('.').append(this.parentcol).
                append(" = ").append(this.table.name).append(".").append(this.joincol).append(" ");


        }
    }

    static class SchemaResponse {
        public Schema schema;
        public long workflowid;
        public Table table;
        public String errorMessage;
    }

    public SchemaResource(MacroBaseConf conf) {
        super(conf);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public SchemaResponse getSchema(SchemaRequest schmaReq) {
        SchemaResponse response = new SchemaResponse();
        HttpSession ss = request.getSession();
         try {
            SQLIngester ingester =  (SQLIngester)ss.getAttribute(MacroBaseConf.SESSION_INGESTER);
             Table tableObject = schmaReq.table;
             // check if the join condition is specified in each of the table involved
             tableObject.fillMissingPkColumns(ingester);
             StringBuilder sb = new StringBuilder();
             tableObject.generateQuery(sb, true);
             TableData td = TableManager.getTableData(sb.toString(), ingester, tableObject.isQuery());
             response.schema = td.getSchema();
             response.workflowid = td.getWorkFlowId();
             response.table = tableObject;
        } catch (Exception e) {
            log.error("An error occurred while processing a request: {}", e);
            response.errorMessage = ExceptionUtils.getStackTrace(e);
        }
        return response;
    }
}
