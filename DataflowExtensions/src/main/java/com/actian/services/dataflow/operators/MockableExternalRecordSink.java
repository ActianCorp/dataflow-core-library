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

import com.pervasive.datarush.annotations.OperatorDescription;
import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.record.FullDataDistribution;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.tokens.TokenUtils;
import com.pervasive.datarush.types.RecordTokenType;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;

//@JsonTypeName("mockExternalRecordSink")
@JsonAutoDetect(JsonMethod.NONE)
@OperatorDescription("A sink for external data.")
public class MockableExternalRecordSink extends ExecutableOperator implements RecordPipelineOperator {

	private final RecordPort input = newRecordInput("input");
	private final RecordPort output = newRecordOutput("output");
	
	@PortDescription("Input data")
	public RecordPort getInput() {
		return input;
	}
	
	@PortDescription("Output data")
	public RecordPort getOutput() {
		return output;
	}

	@Override
	protected void execute(ExecutionContext ctx) {
		// Pass through
		RecordInput inputRec   = (RecordInput) ctx.getInputPort(getInput());
		RecordOutput outputRec = null;
		try {
			outputRec = (RecordOutput) ctx.getOutputPort(getOutput());
		} catch (NullPointerException ex) {
			; // Silently ignore
		}
		
		if (outputRec != null) {
			while (inputRec.stepNext()) {
                TokenUtils.transfer(inputRec.getFields(), outputRec.getFields());
				outputRec.push();
			}
			outputRec.pushEndOfData();
		} else {
			while (inputRec.stepNext()) {
				;
			}
		}

	}

	@Override
	protected void computeMetadata(StreamingMetadataContext ctx) {
		// Confirm that there is no redistribution
		// Check for input
		RecordTokenType schema;
		schema = input.getCombinedMetadata(ctx).getType();
		output.setType(ctx, schema);
		getOutput().setOutputDataDistribution(ctx, FullDataDistribution.INSTANCE);
	}

}
