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
import com.pervasive.datarush.namespace.Namespace;
import com.pervasive.datarush.operators.ExecutableOperator;
import com.pervasive.datarush.operators.ExecutionContext;
import com.pervasive.datarush.operators.MetadataContext;
import com.pervasive.datarush.operators.ParallelismStrategy;
import com.pervasive.datarush.operators.RecordPipelineOperator;
import com.pervasive.datarush.operators.StreamingMetadataContext;
import com.pervasive.datarush.ports.LogicalPort;
import com.pervasive.datarush.ports.physical.RecordInput;
import com.pervasive.datarush.ports.physical.RecordOutput;
import com.pervasive.datarush.ports.physical.ScalarInputField;
import com.pervasive.datarush.ports.record.FullDataDistribution;
import com.pervasive.datarush.ports.record.RecordPort;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.TypeUtil;

import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;

@OperatorDescription("Merge data by appending columns.")
public class ZipRows extends ExecutableOperator implements RecordPipelineOperator {

    private static final int MAX_INPUTS = 5;
    public final RecordPort output = newRecordOutput("output");
    @JsonIgnore
    public final List<RecordPort> inputs = new ArrayList<>(MAX_INPUTS);
    private int portCount = 1;

    public ZipRows() {
        inputs.add(newRecordInput("input"));
        for (int i = 1; i <= 15; i++) {
            inputs.add(newRecordInput("input" + i, true));
        }
    }

    @Override
    protected void execute(ExecutionContext ctx) {
        RecordOutput out = output.getOutput(ctx);

        boolean isdata = false;

        for (RecordPort port : this.getConnectedPorts(ctx)) {
            isdata = isdata | ((RecordInput) ctx.getInputPort(port)).stepNext();
        }

        while (isdata) {

            int fieldIdx = 0;
            for (RecordPort p : this.getConnectedPorts(ctx)) {
                for (int j = 0; j < p.getType(ctx).size(); j++) {
                    ScalarInputField sif = p.getInput(ctx).getField(j);
                    if (p.getInput(ctx).isOnToken()) {
                        out.getField(fieldIdx).set(sif);
                    }
                    fieldIdx++;
                }
            }
            out.push();
            isdata = false;
            for (RecordPort port : this.getConnectedPorts(ctx))
            {
                if (((RecordInput) ctx.getInputPort(port)).isOnToken()) {
                    isdata = isdata | ((RecordInput) ctx.getInputPort(port)).stepNext();
                }
            }
        }
        out.pushEndOfData();
    }

    public List<RecordPort> getConnectedPorts(MetadataContext ctx) {
        List l = new ArrayList<>();
        Namespace<LogicalPort> ps = getInputPorts();
        for (LogicalPort p : ps.toList()) {
            if (ctx.isSourceConnected(p)) {
                l.add(p);
            }
        }
        return l;
    }

    @Override
    @PortDescription("Mandatory input")
    public RecordPort getInput() {
        return inputs.get(0);
    }

    @Override
    public RecordPort getOutput() {
        return output;
    }

    public RecordPort newInput() {
        return inputs.get(portCount++);
    }

    @Override
    protected void computeMetadata(StreamingMetadataContext ctx) {
        RecordTokenType schema = null;
        
        ctx.parallelize(ParallelismStrategy.NON_PARALLELIZABLE);
        for (RecordPort p : getConnectedPorts(ctx)) {
            schema = schema != null ? TypeUtil.merge(schema, p.getType(ctx)) : p.getType(ctx);
            p.setRequiredDataDistribution(ctx, FullDataDistribution.INSTANCE);
        }
        output.setType(ctx, schema);
    }

}
