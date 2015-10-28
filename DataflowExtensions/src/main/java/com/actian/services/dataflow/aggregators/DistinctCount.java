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

import java.util.List;

import com.pervasive.datarush.DRException;
import com.pervasive.datarush.operators.group.Aggregator;
import com.pervasive.datarush.operators.group.AggregatorFactory;
import com.pervasive.datarush.operators.group.InvalidArgumentCountException;
import com.pervasive.datarush.types.ScalarTokenType;

public final class DistinctCount implements AggregatorFactory {

	public DistinctCount() {
	}
	
    @Override
	public String getKey() {
		return "dstrcount";
	}

    @Override
	public Aggregator newAggregator(List<String> inputNames, ScalarTokenType[] inputs) {
		if (inputs.length != 1) {
			throw new InvalidArgumentCountException(getKey(),1, inputs.length);
		}
		
		ScalarTokenType valueType = inputs[0];
		
		if (valueType.isScalar()) {
			return new DistinctCountString();
		} else {
			throw new DRException("dstrcount is unsupported for token type " + valueType + " for field " + inputNames.get(0));
		}
	}

}
