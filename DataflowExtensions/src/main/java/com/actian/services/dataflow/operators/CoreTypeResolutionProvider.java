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

import com.pervasive.datarush.json.SimpleTypeResolutionProvider;

public class CoreTypeResolutionProvider extends SimpleTypeResolutionProvider {
   public CoreTypeResolutionProvider() {
       register(LeadLag.class);
       register(Sessionize.class);

       // Commented out below operators as being potentially too fiddly to work
       // with in RushScript. (not RushScript friendly)
       // However can still be referenced using dr.defineOperator(..., ...)

       //register(ZipRows.class);
       //register(MockableExternalRecordSink.class);
       //register(MockableExternalRecordSource.class);
       //register(SubJobExecutor.class);
       //register(XPathTable.class);
   }  
}
