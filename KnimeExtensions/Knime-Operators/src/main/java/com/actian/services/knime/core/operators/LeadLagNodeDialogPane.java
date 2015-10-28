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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;

import com.actian.services.dataflow.operators.LeadLag;
import com.pervasive.datarush.coreui.common.MultiSelectPanel;
import com.pervasive.datarush.knime.coreui.common.CustomDialogComponent;
import com.pervasive.datarush.ports.PortMetadata;
import com.pervasive.datarush.ports.record.RecordMetadata;
import com.pervasive.datarush.types.RecordTokenType;

import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JSeparator;

final class LeadLagNodeDialogPane extends JPanel implements CustomDialogComponent<LeadLag> {

	private LeadLagNodeSettings settings = new LeadLagNodeSettings();
	private MultiSelectPanel columnSelect = new MultiSelectPanel(true);
	JComboBox<String> comboValue = new JComboBox<String>();
	JComboBox<String> comboDirection = new JComboBox<String>();
	private RecordTokenType inType;
	
	public LeadLagNodeDialogPane() {
		
		
		JLabel lblOrderField = new JLabel("Order Field:");
		JLabel lblDirection = new JLabel("Direction:");
		
		comboDirection.setModel(new DefaultComboBoxModel(new String[] {"ASC", "DESC"}));
		
		JSeparator separator = new JSeparator();
		GroupLayout groupLayout = new GroupLayout(this);
		groupLayout.setHorizontalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(groupLayout.createSequentialGroup()
					.addContainerGap()
					.addGroup(groupLayout.createParallelGroup(Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
							.addComponent(lblDirection)
							.addGap(18)
							.addComponent(comboDirection, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(groupLayout.createSequentialGroup()
							.addComponent(lblOrderField, GroupLayout.PREFERRED_SIZE, 59, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(comboValue, GroupLayout.PREFERRED_SIZE, 124, GroupLayout.PREFERRED_SIZE))
						.addComponent(columnSelect, GroupLayout.DEFAULT_SIZE, 389, Short.MAX_VALUE)
						.addComponent(separator, GroupLayout.DEFAULT_SIZE, 389, Short.MAX_VALUE))
					.addContainerGap())
		);
		groupLayout.setVerticalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(groupLayout.createSequentialGroup()
					.addContainerGap()
					.addComponent(columnSelect, GroupLayout.PREFERRED_SIZE, 248, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(separator, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addGroup(groupLayout.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblOrderField)
						.addComponent(comboValue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(groupLayout.createParallelGroup(Alignment.BASELINE)
						.addComponent(lblDirection)
						.addComponent(comboDirection, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
		);
		setLayout(groupLayout);
	}
	
	@Override
	public LeadLagNodeSettings getSettings() {
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
		inType = ((RecordMetadata)specs[0]).getType();
		
		List<String> inputFields = Arrays.asList(inType.getNames());
		List<String> currentKeyFields = new ArrayList<>();
		currentKeyFields.addAll(Arrays.asList(this.settings.keys.getStringArrayValue()));
		List<String> nonKeyFields = new ArrayList<>();
		nonKeyFields.addAll(inputFields);
		nonKeyFields.removeAll(currentKeyFields);
		currentKeyFields.retainAll(inputFields);
		
		columnSelect.setExcludeList(nonKeyFields);
		columnSelect.setIncludeList(currentKeyFields);
		comboValue.removeAllItems();
		for (String item : inputFields) {
			comboValue.addItem(item);
		}
		comboValue.setSelectedItem(this.settings.valueField.getStringValue());
		comboDirection.setSelectedItem(this.settings.direction.getStringValue());
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		this.settings.keys.setStringArrayValue(columnSelect.getIncludeList().toArray(new String[]{}));
		this.settings.valueField.setStringValue((String) comboValue.getSelectedItem());
		this.settings.direction.setStringValue((String)comboDirection.getSelectedItem());
	}
}
