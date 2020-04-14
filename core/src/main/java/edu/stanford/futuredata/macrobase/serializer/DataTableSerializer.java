package edu.stanford.futuredata.macrobase.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Lists;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableCell;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.render.EscapeUtil;
import com.google.visualization.datasource.render.JsonRenderer;
import org.apache.commons.lang.text.StrBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.visualization.datasource.render.JsonRenderer.appendCellJson;
import static com.google.visualization.datasource.render.JsonRenderer.appendColumnDescriptionJson;

public class DataTableSerializer extends StdSerializer<DataTable> {
    public DataTableSerializer() {
        this(null);
    }
    public DataTableSerializer(Class<DataTable> t) {
        super(t);
    }
    @Override
    public void serialize(DataTable dataTable, JsonGenerator jgen, SerializerProvider serializerProvider) throws IOException {
        //jgen.writeStartObject();
        jgen.writeRaw(JsonRenderer.renderDataTable( dataTable, true, true).toString());

       //jgen.writeEndObject();
    }






}
