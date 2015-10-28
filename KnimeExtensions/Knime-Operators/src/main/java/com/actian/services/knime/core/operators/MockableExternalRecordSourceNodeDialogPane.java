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
package com.actian.services.knime.core.operators;

import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;

import com.pervasive.datarush.knime.core.framework.AbstractDRNodeDialogPane;
import com.actian.services.dataflow.operators.MockableExternalRecordSource;

/*package*/ final class MockableExternalRecordSourceNodeDialogPane extends AbstractDRNodeDialogPane<MockableExternalRecordSource> {

    private final MockableExternalRecordSourceNodeSettings settings = new MockableExternalRecordSourceNodeSettings();
        
    public MockableExternalRecordSourceNodeDialogPane() {
        super( new MockableExternalRecordSource() );

        DialogComponentMultiLineString propXml = new DialogComponentMultiLineString(settings.propXml, "Properties XML", true, 60, 20);
        addDialogComponent(propXml);
        
        setDefaultTabTitle("Start Node Properties");

    }

    @Override
    protected MockableExternalRecordSourceNodeSettings getSettings() {
        return settings;
    }
    
    @Override
    protected boolean isMetadataRequiredForConfiguration(int portIndex) {
        return true;
    }
    
}

