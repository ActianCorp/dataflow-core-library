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
import com.pervasive.datarush.annotations.OperatorDescription;
import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.annotations.PropertyDescription;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.TimestampInputField;
import com.pervasive.datarush.ports.record.DataOrdering;
import com.pervasive.datarush.ports.record.MetadataUtil;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.tokens.TokenOrder;
import com.pervasive.datarush.tokens.TokenUtils;
import com.pervasive.datarush.tokens.record.SortKey;
import com.pervasive.datarush.tokens.scalar.IntToken;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.TokenTypeConstant;
import static com.pervasive.datarush.types.TokenTypeConstant.INT;
import static com.pervasive.datarush.types.TokenTypeConstant.record;
import com.pervasive.datarush.types.TypeUtil;
import java.sql.Timestamp;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

@JsonAutoDetect(JsonMethod.NONE)
@JsonTypeName("sessionize")
@OperatorDescription("Sessionize input data")
public class Sessionize extends ExecutableOperator implements RecordPipelineOperator {

    private final RecordPort input = newRecordInput("input");
	private final RecordPort output = newRecordOutput("output");
    private String keyColumn;
    private String timeColumn;
    private long interval;
    
    @Override
    protected void execute(ExecutionContext ctx) {
        RecordInput inputRec   = (RecordInput) ctx.getInputPort(getInput());
		RecordOutput outputRec = (RecordOutput) ctx.getOutputPort(getOutput());
        
        int sessionId=0;
        String lastKey = null;
        Timestamp lastTimestamp = new Timestamp(0);
        int numFields = inputRec.getFields().length;
        while (inputRec.stepNext()){
            String key = TokenUtils.asString(inputRec.getField(keyColumn));
            Timestamp now = ((TimestampInputField)inputRec.getField(timeColumn)).asTimestamp();
            if (lastTimestamp.getTime() - now.getTime() >= interval ) {
                sessionId++;
            }
            
            if (key.equals(lastKey) == false)
            {
                sessionId = 1;
            }
            
            lastKey = key;
            lastTimestamp = now;
            
            for(int i = 0; i<numFields;i++) {
                outputRec.getField(i).set(inputRec.getField(i));
            }
            outputRec.getField(numFields).set(new IntToken(sessionId));
            outputRec.push();
        }
        outputRec.pushEndOfData();
        
    }

    @Override
    protected void computeMetadata(StreamingMetadataContext ctx) {
        RecordTokenType schema;

        ctx.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);
        // Does the input have the key field?
        schema = TypeUtil.merge(input.getCombinedMetadata(ctx).getType(),record(INT("session_id")));
        if (schema.get(keyColumn) == null) {
            throw new DRException("Field `" + keyColumn + "` is not a part of the input.");
        }
        if (schema.get(timeColumn) == null) {
            throw new DRException("Field `" + timeColumn + "` is not a part of the input.");
        }
        
        if (schema.get(timeColumn).getType() != TokenTypeConstant.TIMESTAMP ) {
            throw new DRException("Field `" + timeColumn + "` must be a timestamp.");
        }
        
        input.setRequiredDataDistribution(ctx, MetadataUtil.negotiateGrouping(ctx, input, keyColumn));
        input.setRequiredDataOrdering(ctx, new DataOrdering(new SortKey[]{ new SortKey(keyColumn,TokenOrder.ASCENDING), new SortKey(timeColumn, TokenOrder.DESCENDING)}));
        output.setType(ctx, schema);
    }

    @PortDescription("Unsessionized data.")
    public RecordPort getInput() {
        return input;
    }

    @PortDescription("Sessionized data.")
    public RecordPort getOutput() {
        return output;
    }

    @JsonProperty
	@PropertyDescription("Key columns to sessionize within.")
    public String getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    @JsonProperty
	@PropertyDescription("Column containing timestamp of data.")
    public String getTimeColumn() {
        return timeColumn;
    }

    public void setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn;
    }

    @JsonProperty
	@PropertyDescription("Interval before a new session identifier is assigned in milliseconds.")
    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }
    
}
