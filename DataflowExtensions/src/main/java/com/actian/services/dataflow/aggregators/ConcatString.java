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
package com.actian.services.dataflow.aggregators;


import com.pervasive.datarush.operators.group.Aggregator;
import com.pervasive.datarush.operators.group.ReadableStorage;
import com.pervasive.datarush.operators.group.WriteableStorage;
import com.pervasive.datarush.tokens.scalar.ScalarSettable;
import com.pervasive.datarush.tokens.scalar.ScalarValued;
import com.pervasive.datarush.tokens.scalar.StringSettable;
import com.pervasive.datarush.tokens.scalar.StringValued;
import com.pervasive.datarush.types.ScalarTokenType;
import com.pervasive.datarush.types.TokenTypeConstant;
import com.pervasive.datarush.util.StringUtil;
import java.util.ArrayList;
import java.util.List;

public final class ConcatString implements Aggregator {

	private StringValued input;
	private List<String> value = new ArrayList<>();
    private String separator;

    ConcatString(String sep) {
        separator=sep;
    }

    @Override
	public void accumulate() {
		if (!input.isNull()) {
            value.add(input.asString());
		}
	}

    @Override
	public void combineInternals(ReadableStorage internals) {
		value.addAll((ArrayList<String>) internals.getObject(0));
	}

    @Override
	public ScalarTokenType[] getInternalTypes() {
		return new ScalarTokenType[] {
				TokenTypeConstant.OBJECT(ArrayList.class)
		};
	}

    @Override
	public ScalarTokenType getOutputType() {
		return TokenTypeConstant.STRING;
	}

    @Override
	public void reset() {
		value = new ArrayList<>();
	}

    @Override
	public void setInputs(ScalarValued[] inputs) {
		if (inputs.length != 2) {
		    throw new IllegalArgumentException(getClass().getSimpleName() + " expects two inputs.");
		}
		this.input = (StringValued)inputs[0];
	}

    @Override
	public void storeFinalResult(ScalarSettable output) {
		((StringSettable)output).set(StringUtil.join(separator, value.toArray()));
	}

    @Override
	public void storeInternals(WriteableStorage internals) {
		internals.setObject(0,value);
	}

    @Override
	public void updateInternals(ReadableStorage readView, WriteableStorage writeView) {
		storeInternals(writeView);
	}

}
