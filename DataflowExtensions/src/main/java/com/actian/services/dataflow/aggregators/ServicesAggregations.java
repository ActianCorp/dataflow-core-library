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

import com.pervasive.datarush.annotations.Function;
import com.pervasive.datarush.annotations.FunctionArgument;
import com.pervasive.datarush.functions.ConstantReference;
import com.pervasive.datarush.functions.FieldReference;
import com.pervasive.datarush.functions.ScalarValuedFunction;
import com.pervasive.datarush.operators.group.Aggregation;
import com.pervasive.datarush.tokens.scalar.StringToken;
import java.util.Arrays;

public final class ServicesAggregations {

	@Function(name = "dstrcount", description="Counts distinct string values")
	public static Aggregation countdistinct(
		@FunctionArgument(name="input", description="The values to count.") ScalarValuedFunction value)
	{
		return new Aggregation(new DistinctCount(), value);
	}
    
    public static Aggregation countdistinct(String value) {
        return countdistinct(FieldReference.value(value));
    }

    @Function(name = "concat", description="Concatenate string values")
    public static Aggregation concatAggregation(
        @FunctionArgument(name="input",description="Values to concatenate.") ScalarValuedFunction value,
        @FunctionArgument(name="separator", description="Separator") ScalarValuedFunction separator)
    {
        return new Aggregation(new ConcatFactory(), Arrays.asList(value, separator));
    }

    public static Aggregation concatAggregation(ScalarValuedFunction value, String sep) {
        return concatAggregation(value, new ConstantReference(StringToken.parse(sep)));
    }
}
