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

import java.awt.Component;

import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;

import com.actian.services.dataflow.operators.SubJobExecutor;
import com.pervasive.datarush.knime.coreui.common.CustomDialogComponent;
import com.pervasive.datarush.knime.io.DefaultSchemaModel;
import com.pervasive.datarush.knime.io.SchemaEditorPanel;
import com.pervasive.datarush.knime.io.SchemaEditorPanel.EditMode;
import com.pervasive.datarush.knime.io.SchemaModel;
import com.pervasive.datarush.ports.PortMetadata;
import com.pervasive.datarush.schema.TextRecord;


import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;

final class SubJobExecutorNodeDialogPane extends JPanel implements CustomDialogComponent<SubJobExecutor> {

	private final SubJobExecutorNodeSettings settings = new SubJobExecutorNodeSettings();
	private final SchemaEditorPanel schemaPanel;
	private SchemaModel model;
	
	public SubJobExecutorNodeDialogPane() {
		model = new DefaultSchemaModel();
		schemaPanel = new SchemaEditorPanel(model, EditMode.DEFINE);
		GroupLayout groupLayout = new GroupLayout(this);
		groupLayout.setHorizontalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(groupLayout.createSequentialGroup()
					.addComponent(schemaPanel, GroupLayout.DEFAULT_SIZE, 604, Short.MAX_VALUE)
					.addGap(1))
		);
		groupLayout.setVerticalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(Alignment.TRAILING, groupLayout.createSequentialGroup()
					.addContainerGap()
					.addComponent(schemaPanel, GroupLayout.DEFAULT_SIZE, 573, Short.MAX_VALUE)
					.addContainerGap())
		);
		setLayout(groupLayout);
	}
	
	@Override
	public SubJobExecutorNodeSettings getSettings() {
		return settings;
	}

	@Override
	public boolean isMetadataRequiredForConfiguration(int portIndex) {
		return true;
	}

	@Override
	public Component getComponent() {
		return this;
	}

	@Override
	public void refresh(PortMetadata[] specs) {
		model.setSchema(TextRecord.convert(settings.getSchema()));
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		settings.setSchema(model.getSchema().getTokenType());
	}
}
