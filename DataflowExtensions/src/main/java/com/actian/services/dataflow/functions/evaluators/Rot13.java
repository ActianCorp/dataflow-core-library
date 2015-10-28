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
package com.actian.services.dataflow.functions.evaluators;

import com.pervasive.datarush.functions.FunctionEvaluator;
import com.pervasive.datarush.tokens.scalar.StringSettable;
import com.pervasive.datarush.tokens.scalar.StringValued;

public class Rot13 implements FunctionEvaluator {

	private final StringSettable result;
	private final StringValued value;
	
	public Rot13(StringSettable result, StringValued value) {
		this.result = result;
		this.value = value;
	}


    @Override
	public void evaluate() {

		char[] values = value.asString().toCharArray();
		for (int i = 0; i < values.length; i++) {
		    char letter = values[i];

		    if (letter >= 'a' && letter <= 'z') {

			if (letter > 'm') {
			    letter -= 13;
			} else {
			    letter += 13;
			}
		    } else if (letter >= 'A' && letter <= 'Z') {

			if (letter > 'M') {
			    letter -= 13;
			} else {
			    letter += 13;
			}
		    }
		    values[i] = letter;
		}

		result.set(new String(values));
	}

}
