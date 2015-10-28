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
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.actian.services.dataflow.operators.SubJobExecutor;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.TypeUtil;

public class SubJobExecutorNodeSettings extends AbstractDRSettingsModel<SubJobExecutor> {
	
	private RecordTokenType outputSchema;
	private final SettingsModelString outputModel = new SettingsModelString("outputModel","");
	
	@Override
	public void configure(PortMetadata[] inputTypes, SubJobExecutor operator)
			throws InvalidSettingsException {
		operator.setOutputType(outputSchema);
	}

	@Override
	protected List<SettingsModel> getComponentSettings() {
		return Arrays.<SettingsModel>asList(outputModel);
	}

	@Override
	protected void doLoadPostProcess(NodeSettingsRO settings)
			throws InvalidSettingsException {
		super.doLoadPostProcess(settings);
		outputSchema = (RecordTokenType) TypeUtil.fromJSON(outputModel.getStringValue());
	}

	@Override
	protected void doSavePreprocess(NodeSettingsWO settings) {
		super.doSavePreprocess(settings);
		outputModel.setStringValue(TypeUtil.toJSON(outputSchema));
	}

	public RecordTokenType getSchema() {
		return outputSchema;
	}
	
	public void setSchema(RecordTokenType schema) {
		this.outputSchema = schema;
	}
}
