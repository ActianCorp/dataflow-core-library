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

import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;

import com.actian.services.dataflow.operators.Sessionize;
import com.pervasive.datarush.knime.core.framework.AbstractDRNodeDialogPane;

final class SessionizeNodeDialogPane extends AbstractDRNodeDialogPane<Sessionize> {

	private SessionizeNodeSettings settings = new SessionizeNodeSettings();

	public SessionizeNodeDialogPane() {
		super(new Sessionize());

		DialogComponentColumnNameSelection keyColumn = new DialogComponentColumnNameSelection(settings.keyColumn,"Key Column",0,StringValue.class,IntValue.class,LongValue.class);
		DialogComponentColumnNameSelection timeColumn = new DialogComponentColumnNameSelection(settings.timeColumn, "Time Column",0, DateAndTimeValue.class);
		DialogComponentNumber interval = new DialogComponentNumber(settings.interval, "Session Interval", 60);
		DialogComponentStringSelection uot = new DialogComponentStringSelection(settings.uot, "Units", Arrays.asList("Secs", "Mins","Hrs","Days"));
		
		createNewGroup("Session Columns");
		addDialogComponent(keyColumn);
		addDialogComponent(timeColumn);
		createNewGroup("Interval");
		addDialogComponent(interval);
		addDialogComponent(uot);

		setDefaultTabTitle("Sessionize Properties");
	}
	
	@Override
	protected SessionizeNodeSettings getSettings() {
		return settings;
	}

	@Override
	protected boolean isMetadataRequiredForConfiguration(int portIndex) {
		return true;
	}
}
