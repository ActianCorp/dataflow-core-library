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
package com.actian.services.dataflow.functions;

import static com.pervasive.datarush.functions.ScalarFunctionDescriptor.define;

import com.actian.services.dataflow.functions.evaluators.Rot13;
import com.actian.services.dataflow.functions.evaluators.UUID;
import com.pervasive.datarush.annotations.Function;
import com.pervasive.datarush.functions.ScalarValuedFunction;
import com.pervasive.datarush.types.TokenTypeConstant;

public class Fun {
    @Function(description="ROT13 encrypt/decrypt passed in value.",
    		  category="Miscellaneous",
			  argumentNames={"expression"})
    public static ScalarValuedFunction rot13(ScalarValuedFunction expression) {
		return define("Miscellaneous.rot13",TokenTypeConstant.STRING,Rot13.class, expression);
    }
    
    @Function(description="Generate a UUID.",
              category="Miscellaneous")
    public static ScalarValuedFunction uuid() {
        return define("Miscellaneous.uuid",TokenTypeConstant.STRING,UUID.class);
    }
}
