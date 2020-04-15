package edu.stanford.futuredata.macrobase.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Lists;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableCell;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.*;
import com.google.visualization.datasource.render.EscapeUtil;
import com.google.visualization.datasource.render.JsonRenderer;
import com.ibm.icu.util.GregorianCalendar;
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
        jgen.writeString(JsonRenderer.renderDataTable(dataTable, true, true).toString());
      /*  jgen.writeStartObject();
        renderDataTable( jgen, dataTable, true, true);

       jgen.writeEndObject(); */
    }

    private static void renderDataTable(JsonGenerator jgen, DataTable dataTable, boolean includeValues,
                                        boolean includeFormatting) throws IOException {
        if (dataTable.getColumnDescriptions().isEmpty()) {
        } else {
            List<ColumnDescription> columnDescriptions = dataTable.getColumnDescriptions();

            jgen.writeFieldName("cols");
            jgen.writeStartArray();

            for(int colId = 0; colId < columnDescriptions.size(); ++colId) {
                ColumnDescription col = (ColumnDescription)columnDescriptions.get(colId);
                appendColumnDescriptionJson(col, jgen);
            }
            jgen.writeEndArray();

            if (includeValues) {
                jgen.writeFieldName("rows");
                jgen.writeStartArray();
                List<TableRow> rows = dataTable.getRows();

                for(int rowId = 0; rowId < rows.size(); ++rowId) {
                    TableRow tableRow = (TableRow)rows.get(rowId);
                    List<TableCell> cells = tableRow.getCells();
                    jgen.writeStartObject();
                    jgen.writeFieldName("c");
                    jgen.writeStartArray();

                    for(int cellId = 0; cellId < cells.size(); ++cellId) {
                        TableCell cell = (TableCell)cells.get(cellId);
                        if (cellId < cells.size() - 1) {
                            appendCellJson(cell, jgen, includeFormatting, false);

                        } else {
                            appendCellJson(cell, jgen, includeFormatting, true);
                        }
                    }

                    jgen.writeEndArray();
                    writeCustomPropertiesMapString(tableRow.getCustomProperties(), jgen);


                    jgen.writeEndObject();
                }

                jgen.writeEndArray();
            }

            writeCustomPropertiesMapString(dataTable.getCustomProperties(), jgen);



        }
    }

    private static void appendCellJson(TableCell cell, JsonGenerator jgen, boolean includeFormatting,
                                               boolean isLastColumn) throws IOException{
        Value value = cell.getValue();
        ValueType type = cell.getType();
        StringBuilder valueJson = new StringBuilder();
        String escapedFormattedString = "";
        boolean isJsonNull = false;
        if (value != null && !value.isNull()) {
            switch(type) {
                case BOOLEAN:
                    valueJson.append(((BooleanValue)value).getValue());
                    break;
                case DATE:
                    valueJson.append("new Date(");
                    DateValue dateValue = (DateValue)value;
                    valueJson.append(dateValue.getYear()).append(",");
                    valueJson.append(dateValue.getMonth()).append(",");
                    valueJson.append(dateValue.getDayOfMonth());
                    valueJson.append(")");
                    break;
                case NUMBER:
                    valueJson.append(((NumberValue)value).getValue());
                    break;
                case TEXT:
                 //   valueJson.append("'");
                    valueJson.append(EscapeUtil.jsonEscape(value.toString()));
                  //  valueJson.append("'");
                    break;
                case TIMEOFDAY:
                    valueJson.append("[");
                    TimeOfDayValue timeOfDayValue = (TimeOfDayValue)value;
                    valueJson.append(timeOfDayValue.getHours()).append(",");
                    valueJson.append(timeOfDayValue.getMinutes()).append(",");
                    valueJson.append(timeOfDayValue.getSeconds()).append(",");
                    valueJson.append(timeOfDayValue.getMilliseconds());
                    valueJson.append("]");
                    break;
                case DATETIME:
                    GregorianCalendar calendar = ((DateTimeValue)value).getCalendar();
                    valueJson.append("new Date(");
                    valueJson.append(calendar.get(1)).append(",");
                    valueJson.append(calendar.get(2)).append(",");
                    valueJson.append(calendar.get(5));
                    valueJson.append(",");
                    valueJson.append(calendar.get(11));
                    valueJson.append(",");
                    valueJson.append(calendar.get(12)).append(",");
                    valueJson.append(calendar.get(13));
                    valueJson.append(")");
                    break;
                default:
                    throw new IllegalArgumentException("Illegal value Type " + type);
            }
        } else {
            valueJson.append("null");
            isJsonNull = true;
        }

        String formattedValue = cell.getFormattedValue();
        if (value != null && !value.isNull() && formattedValue != null) {
            escapedFormattedString = EscapeUtil.jsonEscape(formattedValue);
            if (type == ValueType.TEXT && value.toString().equals(formattedValue)) {
                escapedFormattedString = "";
            }
        }

        if (isLastColumn || !isJsonNull) {
            jgen.writeStartObject();
            jgen.writeFieldName("v");
            jgen.writeString(valueJson.toString());

            if (includeFormatting && !escapedFormattedString.equals("")) {
                jgen.writeFieldName("f");
                jgen.writeString(escapedFormattedString);
            }

            writeCustomPropertiesMapString(cell.getCustomProperties(), jgen);


           jgen.writeEndObject();
        }


    }

    public static void appendColumnDescriptionJson(ColumnDescription col, JsonGenerator jgen) throws IOException {
        jgen.writeStartObject();
        jgen.writeFieldName("id");
        jgen.writeString(EscapeUtil.jsonEscape(col.getId()));
        jgen.writeFieldName("label");
        jgen.writeString(EscapeUtil.jsonEscape(col.getLabel()));
        jgen.writeFieldName("type");
        jgen.writeString(col.getType().getTypeCodeLowerCase());
        jgen.writeFieldName("pattern");
        jgen.writeString(EscapeUtil.jsonEscape(col.getPattern()));
        writeCustomPropertiesMapString(col.getCustomProperties(), jgen);

       jgen.writeEndObject();
    }

    private static void writeCustomPropertiesMapString(Map<String, String> propertiesMap, JsonGenerator jgen) throws IOException {
       // String customPropertiesString = null;

        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            jgen.writeFieldName("p");
            jgen.writeStartObject();
          //  List<String> customPropertiesStrings = Lists.newArrayList();
            Iterator i$ = propertiesMap.entrySet().iterator();

            while(i$.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry)i$.next();
               // customPropertiesStrings.add("'" + EscapeUtil.jsonEscape((String)entry.getKey()) + "':'" + EscapeUtil.jsonEscape((String)entry.getValue()) + "'");
                jgen.writeFieldName("'" + EscapeUtil.jsonEscape((String)entry.getKey()) + "'");
                jgen.writeString("'" + EscapeUtil.jsonEscape((String)entry.getValue()) + "'");
            }
            jgen.writeEndObject();
            //customPropertiesString = (new StrBuilder("{")).appendWithSeparators(customPropertiesStrings, ",").append("}").toString();
        }

        //return customPropertiesString;
    }




}
