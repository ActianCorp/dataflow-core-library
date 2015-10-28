/*
   Copyright 2015 Actian Corporation
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.actian.services.dataflow.operators;

import com.pervasive.datarush.DRException;
import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.graphs.EngineConfig;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphInstance;
import com.pervasive.datarush.io.FileClient;
import com.pervasive.datarush.io.Paths;
import com.pervasive.datarush.json.JSON;
import com.pervasive.datarush.operators.AbstractExecutableRecordPipeline;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.LogicalPort;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.physical.StringInputField;
import com.pervasive.datarush.ports.record.ExternalRecordSink;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.tokens.scalar.StringValued;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.TokenTypeConstant;
import com.pervasive.datarush.util.FileUtil;
import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class SubJobExecutor extends AbstractExecutableRecordPipeline {

    public static final String DB_URL = "Global!dbUrl";
    public static final String DB_USER = "Global!dbUser";
    public static final String DB_PASSWORD = "Global!dbPassword";
    public static final String DB_DRIVERNAME = "Global!dbDriverName";

    private RecordTokenType outputType;
    private final RecordPort overrides = newRecordInput("overrides",true);

    @Override
    protected void execute(ExecutionContext ctx) {
        EngineConfig config = ctx.getEngineConfig();

        RecordInput inputRec = (RecordInput) ctx.getInputPort(getInput());
        RecordOutput outputRec = (RecordOutput) ctx.getOutputPort(getOutput());

        RecordInput overrideRec = null;
        try {
            overrideRec = (RecordInput) ctx.getInputPort(getOverrides());
        } catch (NullPointerException ex) {
            // Ignore
        }

        /* Store the overrides */
        Map<String,String> dict = new HashMap<>();
        if (overrideRec != null) {
            while (overrideRec.stepNext()) {
                dict.put(((StringInputField)(overrideRec.getField(0))).asString(),
                         ((StringInputField)(overrideRec.getField(1))).asString());
            }
        }

        while (inputRec.stepNext()) {
            JSON json = new JSON();
            LogicalGraph graph = null;
            String fileURL = ((StringInputField)inputRec.getField(0)).asString();
            try (InputStream is = ctx.getFileClient().newInputStream(Paths.asPath(fileURL))) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(is);
                JsonNode graphNode = root.findPath("logicalGraph");
                graphNode = graphNode.isMissingNode() ? root : graphNode;
                // Find all nodes which are have @type : "readFromJDBC"
                overrideSettings(graphNode, dict, mapper);

                graph = json.parse(graphNode.toString(), LogicalGraph.class);

                if (graph != null) {
                    /* Override graph properties */
                    for(String key : dict.keySet()) {
                        try {
                            graph.setProperty(key, dict.get(key));
                        }
                        catch (IllegalArgumentException ex) {
                            // Ignore
                        }
                    }
                    ExternalRecordSink collector = new ExternalRecordSink();

                    Properties params = new Properties();

                    for (int i = 0; i < inputRec.getFields().length; i++) {
                        ScalarInputField sif = inputRec.getField(i);
                        if (sif instanceof StringValued) {
                            params.put(sif.getName(),((StringValued)sif).asString() );
                        } else {
                            params.put(sif.getName(), sif.toString());
                        }
                    }

                    // Find the start node.
                    LogicalPort p = (LogicalPort) graph.getProperty("Start Node.output");
                    MockableExternalRecordSource opStart = (MockableExternalRecordSource) p.getOwner();

                    try {
                        // Find the end node.
                        LogicalPort q = (LogicalPort) graph.getProperty("Stop Node.output");

                        RecordPipelineOperator opFinish = (RecordPipelineOperator) q.getOwner();

                        // Add new nodes
                        graph.add(collector, "collector");
                        graph.connect(opFinish.getOutput(), collector.getInput());

                        // Setup custom input
                        opStart.getProperties().putAll(params);
                    } catch (Exception ex) {
                        ; // Ignore if no Stop node.
                    }

                    LogicalGraphInstance gi = graph.compile(config);
                    gi.start();

                    RecordInput rec = collector.getOutput();
                    while (rec.stepNext()) {
                        for (int i = 0; i < outputRec.getFields().length; i++) {

                            ScalarInputField value = rec.getField(outputRec.getField(i).getName());
                            if (value != null) {
                                if (value instanceof StringValued) {
                                    outputRec.getField(i).set(value);
                                } else {
                                    outputRec.getField(i).set(value);
                                }
                            } else {
                                outputRec.getField(i).setNull();
                            }
                        }
                        outputRec.push();
                    }
                    gi.join();
                }
            } catch (Exception ex) {
                throw new DRException(ex.getMessage());
            }
        }
        outputRec.pushEndOfData();
    }

    @Override
    protected void computeMetadata(StreamingMetadataContext ctx) {
        ctx.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);

        RecordTokenType inputType =  input.getType(ctx);
        if (inputType.isEmpty()) {
            throw new DRException("Input record has no fields.");
        } else if (inputType.get(0).getType() != TokenTypeConstant.STRING) {
            throw new DRException("Initial field should be a string.");
        } else if (outputType == null) {
            throw new DRException("An output schema is required.");
        }
        output.setType(ctx, outputType);
    }

    @PortDescription("Graph overrides")
    public RecordPort getOverrides() {
        return overrides;
    }

    public RecordTokenType getOutputType() {
        return outputType;
    }

    public void setOutputType(RecordTokenType outputType) {
        this.outputType = outputType;
    }

    private static void overrideSettings(JsonNode graphNode, Map<String, String> overrides, ObjectMapper mapper) {
            List<JsonNode> composites = graphNode.findValues("operators");
            for (JsonNode compNode : composites) {
                List<JsonNode> operators = compNode.findValues("operator");
                for (JsonNode operator : operators) {
                    if (operator.isObject()) {
                        // Check if its a JDBC node
                        if (operator.path("jdbcConnector").isObject()) {
                            ObjectNode jdbc = (ObjectNode) operator.path("jdbcConnector");
                            if (overrides.containsKey(DB_USER)) {
                                jdbc.put("user", (String) overrides.get(DB_USER));
                            }
                            if (overrides.containsKey(DB_URL)) {
                                jdbc.put("url", (String) overrides.get(DB_URL));
                            }
                            if (overrides.containsKey(DB_DRIVERNAME)) {
                                jdbc.put("driverName", (String) overrides.get(DB_DRIVERNAME));
                            }
                            if (overrides.containsKey(DB_PASSWORD)) {
                                ObjectNode passwordNode = mapper.createObjectNode();
                                passwordNode.put("encryptedText", (String) overrides.get(DB_PASSWORD));
                                passwordNode.put("provider", "notencrypted");
                                jdbc.put("password", passwordNode);
                            }
                        }
                    }
                }
                overrideSettings(compNode, overrides, mapper);
            }
        }
}
