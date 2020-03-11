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
        public String alias;
        public List<Join> joinlist;
        public void fillMissingPkColumnsAndAlias(SQLIngester ingester, int level, boolean isTopNode,
            String parentTableAlias) {

          if (isTopNode) {
            alias = "table_" + level;
          } else {
            alias = parentTableAlias + "_" + level;
          }
          if (!(joinlist == null || joinlist.isEmpty())) {
              int i = 0;
              for(Join join: joinlist) {
                join.fillMissingPkColumnsAndAlias(this, ingester, i++);
              }
          }
        }

        public boolean isQuery() {
          return !(this.joinlist == null || this.joinlist.isEmpty());
        }

        public void generateQuery(StringBuilder sb, boolean isTopNode) {
          if (isTopNode) {
              if (isQuery()) {
                  sb.append("select * from ").append(this.name).append(" as ").append(this.alias).append(" ");
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
        public void fillMissingPkColumnsAndAlias(Table parentTable, SQLIngester ingester, int level) {
          if (this.jointype == null || this.jointype.trim().isEmpty()) {
              this.jointype = "inner join";
          }
          if (parentcol == null || parentcol.trim().isEmpty()) {
            TableData td = TableManager.getTableData(parentTable.name.toLowerCase(), ingester, false);
            TableData childTd = TableManager.getTableData(table.name.toLowerCase(), ingester, false);
            if (joincol != null && !joincol.trim().isEmpty()) {
              TableData.ColumnData childCol = childTd.getColumnData(joincol.toLowerCase());
              this.parentcol = td.getFirstAvailablePkColumn(childCol.sqlType).name;
            } else {
              this.parentcol = td.getFirstAvailablePkColumn().name;
              this.joincol = childTd.getFirstAvailablePkColumn().name;
            }
          } else {
              TableData td = TableManager.getTableData(parentTable.name.toLowerCase(), ingester, false);
              TableData childTd = TableManager.getTableData(table.name.toLowerCase(), ingester, false);
              if (joincol == null || joincol.trim().isEmpty()) {
                  TableData.ColumnData parentCol = td.getColumnData(parentcol.toLowerCase());
                  this.joincol = childTd.getFirstAvailablePkColumn(parentCol.sqlType).name;
              }
          }
          table.fillMissingPkColumnsAndAlias(ingester, level,false, parentTable.alias);
        }

        public void generateQuery(StringBuilder sb, Table parentTable) {
                sb.append(this.jointype).append(" ").append(this.table.name).append(" as ").append(this.table.alias).
                append(" on ").append(parentTable.alias).append('.').append(this.parentcol).
                append(" = ").append(this.table.alias).append(".").append(this.joincol).append(" ");
                this.table.generateQuery(sb, false);

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
             tableObject.fillMissingPkColumnsAndAlias(ingester, 0, true, "");
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
