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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.actian.services.dataflow.operators.MockableExternalRecordSource;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;

/*package*/ final class MockableExternalRecordSourceNodeSettings extends AbstractDRSettingsModel<MockableExternalRecordSource> {

	public final SettingsModelString propXml = new SettingsModelString("propXml","<!DOCTYPE properties SYSTEM 'http://java.sun.com/dtd/properties.dtd'>\n<properties>\n  <entry key='body'></entry>\n</properties>");
        
    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(propXml);
    }

    @Override
    public void configure(PortMetadata[] inputTypes,
            MockableExternalRecordSource operator) throws InvalidSettingsException { 
    	Properties p = new Properties();
    	try {
			p.loadFromXML(new ByteArrayInputStream(propXml.getStringValue().getBytes("UTF-8")));
		} catch (InvalidPropertiesFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	operator.setProperties(p);
    }

}
