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
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.actian.services.dataflow.operators.Sessionize;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;

public class SessionizeNodeSettings extends AbstractDRSettingsModel<Sessionize> {

	public final SettingsModelString keyColumn = new SettingsModelString("keyColumn", "");
	public final SettingsModelString timeColumn = new SettingsModelString("timeColumn", "");
	public final SettingsModelInteger interval = new SettingsModelInteger("interval", 60);
	public final SettingsModelString uot = new SettingsModelString("uot", "Secs");
	
	@Override
	public void configure(PortMetadata[] inputTypes, Sessionize operator)
			throws InvalidSettingsException {
		operator.setKeyColumn(keyColumn.getStringValue());
		operator.setTimeColumn(timeColumn.getStringValue());
		long i = interval.getIntValue()*1000;
		switch(uot.getStringValue()) {
			case "Secs": i = i * 1; break;
			case "Mins": i = i * 60; break;
			case "Hrs" : i = i * 60 * 60; break;
			case "Days": i = i * 60 * 60 * 24; break;
		}
		operator.setInterval(i);
	}

	@Override
	protected List<SettingsModel> getComponentSettings() {
		return Arrays.<SettingsModel>asList(keyColumn,timeColumn,interval,uot);
	}

}
