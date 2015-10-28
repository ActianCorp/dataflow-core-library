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

import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.operators.group.Group;
import com.pervasive.datarush.ports.PortMetadata;

public class DeriveGroupNodeSettings extends AbstractDRSettingsModel<Group> {

	public final SettingsModelString expression = new SettingsModelString("expression","count() as mycount");
	public final SettingsModelStringArray keys = new SettingsModelStringArray("keys",new String[]{});
	
	@Override
	public void configure(PortMetadata[] inputTypes, Group operator)
			throws InvalidSettingsException {
		operator.setAggregations(expression.getStringValue());
		String[] keyValues = keys.getStringArrayValue();
		operator.setKeys(keyValues);
	}

	@Override
	protected List<SettingsModel> getComponentSettings() {
		return Arrays.<SettingsModel>asList(expression,keys);
	}

}
