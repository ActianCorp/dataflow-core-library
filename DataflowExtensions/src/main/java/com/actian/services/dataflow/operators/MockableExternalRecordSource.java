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

import static com.pervasive.datarush.types.TokenTypeConstant.STRING;

import java.util.Properties;

import com.pervasive.datarush.annotations.OperatorDescription;
import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.annotations.PropertyDescription;
import com.pervasive.datarush.graphs.DROperatorException;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.StringOutputField;
import com.pervasive.datarush.ports.record.FullDataDistribution;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.tokens.TokenUtils;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import java.io.FileInputStream;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonProperty;

//@JsonTypeName("mockExternalRecordSource")
@JsonAutoDetect(JsonMethod.NONE)
@OperatorDescription("A source for external data.")
public class MockableExternalRecordSource extends ExecutableOperator implements RecordPipelineOperator {

	private final RecordPort input = newRecordInput("input", true);
	private final RecordPort output = newRecordOutput("output");
	private Properties properties = new Properties();
    private String parameterFile = "";
	
	@PortDescription("Mock input data")
	public RecordPort getInput() {
		return input;
	}
	
	@PortDescription("Output data")
	public RecordPort getOutput() {
		return output;
	}
	
	@JsonProperty
	@PropertyDescription("A collection of name/value out parameters")
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

    @JsonProperty
    public String getParameterFile() {
        return parameterFile;
    }

    public void setParameterFile(String filename) {
        this.parameterFile = filename;
    }

	@Override
	protected void execute(ExecutionContext ctx) {
		// Pass through
		RecordInput inputRec = null;
		try {
			inputRec = (RecordInput) ctx.getInputPort(getInput());
		} catch (NullPointerException ex) {
			; // Get this exception if no input.
		}

        // Load properties from a file?
        if (parameterFile != null && !parameterFile.isEmpty()) {
            try {
                properties.loadFromXML(new FileInputStream(parameterFile));
            } catch (IOException ex) {
                throw new DROperatorException(ctx.getPath(), ex);
            }
        }

		RecordOutput outputRec = (RecordOutput) ctx.getOutputPort(getOutput());
		if (inputRec != null) {
			while (inputRec.stepNext()) {
                TokenUtils.transfer(inputRec.getFields(), outputRec.getFields());
				outputRec.push();
			}
		} else {
			for (Object key : properties.keySet()) {
				StringOutputField sof = (StringOutputField) outputRec.getField((String) key);
				if (sof != null) {
					sof.set(properties.getProperty((String) key));
				}
			}
			outputRec.push();
		}
		outputRec.pushEndOfData();
	}

	@Override
	protected void computeMetadata(StreamingMetadataContext ctx) {
		// Confirm that there is no redistribution
		// Check for input
		RecordTokenType schema;
		try {
			schema = input.getCombinedMetadata(ctx).getType();
		} catch (IllegalStateException ex) {
			RecordTokenTypeBuilder builder = new RecordTokenTypeBuilder();
			
			for (Object key : properties.keySet()) {
				builder.addField((String) key, STRING);
			}
			schema = builder.toType();
		}
		// Confirm schema is all string type
		output.setType(ctx, schema );
		getOutput().setOutputDataDistribution(ctx, FullDataDistribution.INSTANCE);
	}

}
