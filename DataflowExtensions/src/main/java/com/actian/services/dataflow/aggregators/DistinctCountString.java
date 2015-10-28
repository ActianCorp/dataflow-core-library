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

import java.util.HashSet;
import java.util.Set;

import com.pervasive.datarush.operators.group.Aggregator;
import com.pervasive.datarush.operators.group.ReadableStorage;
import com.pervasive.datarush.operators.group.WriteableStorage;
import com.pervasive.datarush.tokens.scalar.LongSettable;
import com.pervasive.datarush.tokens.scalar.ScalarSettable;
import com.pervasive.datarush.tokens.scalar.ScalarValued;
import com.pervasive.datarush.tokens.scalar.StringValued;
import com.pervasive.datarush.types.ScalarTokenType;
import com.pervasive.datarush.types.TokenTypeConstant;

public final class DistinctCountString implements Aggregator {

	private StringValued input;
	private Set<String> values = new HashSet<String>();
	
    @Override
	public void accumulate() {
		if (!input.isNull()) {
			values.add(input.asString());
		}
	}

    @Override
	public void combineInternals(ReadableStorage internals) {
		values.addAll((HashSet<String>) internals.getObject(0));
	}

    @Override
	public ScalarTokenType[] getInternalTypes() {
		return new ScalarTokenType[] {
				TokenTypeConstant.OBJECT(HashSet.class)
		};
	}

    @Override
	public ScalarTokenType getOutputType() {
		return TokenTypeConstant.LONG;
	}

    @Override
	public void reset() {
		values = new HashSet<>();
	}

    @Override
	public void setInputs(ScalarValued[] inputs) {
		if (inputs.length != 1) {
		    throw new IllegalArgumentException(getClass().getSimpleName() + " expects a single input.");
		}
		this.input = (StringValued)inputs[0];
	}

    @Override
	public void storeFinalResult(ScalarSettable output) {
		((LongSettable)output).set(values.size());

	}

    @Override
	public void storeInternals(WriteableStorage internals) {
		internals.setObject(0,values);
	}

    @Override
	public void updateInternals(ReadableStorage readView, WriteableStorage writeView) {
		storeInternals(writeView);
	}

}
