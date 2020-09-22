package edu.stanford.futuredata.macrobase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.JDBCDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.RESTDataFrameLoader;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.sql.Connection;
import java.util.Map;
import java.util.List;

public class PipelineUtils {
    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes,
            List<String> requiredColumns,
            String baseTable,
            String extraPredicate,
            Connection providedConn
    ) throws Exception {
        return PipelineUtils.loadDataFrame(
                inputURI, colTypes, null, null, false,
                requiredColumns,
                baseTable,
                extraPredicate, providedConn
        );
    }

    public static DataFrame loadDataFrame(
        String inputURI,
        Map<String, Schema.ColType> colTypes,
        List<String> requiredColumns,
        String baseTable,
        String extraPredicate
    ) throws Exception {
        return PipelineUtils.loadDataFrame(
            inputURI, colTypes, null, null, false,
            requiredColumns,
            baseTable,
            extraPredicate, null
        );
    }

    public static DataFrame loadDataFrame(
        String inputURI,
        Map<String, Schema.ColType> colTypes,
        Map<String, String> restHeader,
        Map<String, Object> jsonBody,
        boolean usePost,
        List<String> requiredColumns,
        String baseTable,
        String extraPredicate
    ) throws Exception {
        return PipelineUtils.loadDataFrame(
            inputURI, colTypes, restHeader, jsonBody, usePost, requiredColumns, baseTable,
            extraPredicate,null);
    }


    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes,
            Map<String, String> restHeader,
            Map<String, Object> jsonBody,
            boolean usePost,
            List<String> requiredColumns,
            String baseTable,
            String extraPredicate,
            Connection providedConn
    ) throws Exception {
        if (providedConn == null) {
            if (inputURI.startsWith("csv")) {
                // take off "csv://" from inputURI
                CSVDataFrameParser loader = new CSVDataFrameParser(inputURI.substring(6), requiredColumns);
                loader.setColumnTypes(colTypes);
                DataFrame df = loader.load();
                return df;
            } else if (inputURI.startsWith("http")) {
                ObjectMapper mapper = new ObjectMapper();
                String bodyString = mapper.writeValueAsString(jsonBody);

                RESTDataFrameLoader loader = new RESTDataFrameLoader(
                    inputURI,
                    restHeader,
                    requiredColumns
                );
                loader.setUsePost(usePost);
                loader.setJsonBody(bodyString);
                loader.setColumnTypes(colTypes);
                DataFrame df = loader.load();
                return df;
            } else if (inputURI.startsWith("jdbc")) {
                JDBCDataFrameLoader loader = new JDBCDataFrameLoader(inputURI, baseTable, requiredColumns, extraPredicate);
                loader.setColumnTypes(colTypes);
                DataFrame df = loader.load();
                return df;
            } else {
                throw new MacroBaseException("Unsupported URI");
            }
        } else {
            JDBCDataFrameLoader loader = new JDBCDataFrameLoader(providedConn, baseTable, requiredColumns, extraPredicate);
            loader.setColumnTypes(colTypes);
            DataFrame df = loader.load();
            return df;
        }
    }

    public static Pipeline createPipeline(
            PipelineConfig conf
    ) throws MacroBaseException {
        String pipelineName = conf.get("pipeline");
        switch (pipelineName) {
            case "BasicBatchPipeline": {
                return new BasicBatchPipeline(conf);
            }
            case "CubePipeline": {
                return new CubePipeline(conf);
            }
            default: {
                throw new MacroBaseException("Bad Pipeline");
            }
        }
    }
}
