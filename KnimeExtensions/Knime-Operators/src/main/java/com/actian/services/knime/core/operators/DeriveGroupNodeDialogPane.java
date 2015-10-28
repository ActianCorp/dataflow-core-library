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
import java.awt.Font;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.LayoutStyle.ComponentPlacement;

import org.apache.commons.lang.StringUtils;
import org.knime.core.node.InvalidSettingsException;

import com.pervasive.datarush.aggregations.AggregationRegistry;
import com.pervasive.datarush.coreui.common.MultiSelectPanel;
import com.pervasive.datarush.functions.FunctionDescription;
import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.knime.coreui.common.CustomDialogComponent;
import com.pervasive.datarush.operators.group.Group;
import com.pervasive.datarush.ports.PortMetadata;
import com.pervasive.datarush.ports.record.RecordMetadata;
import com.pervasive.datarush.types.RecordTokenType;

final class DeriveGroupNodeDialogPane extends JPanel implements
CustomDialogComponent<Group> {

	private static final long serialVersionUID = 1L;

	private DeriveGroupNodeSettings settings = new DeriveGroupNodeSettings();

	private final MultiSelectPanel columnSelect = new MultiSelectPanel(true);
	private JPanel keysPanel;
	private JScrollPane expressionPanel;
	private RecordTokenType inType;
	private JTextArea expression;
	private JComboBox<String> fcomboBox;
	private JPanel functionPanel;
	
	public DeriveGroupNodeDialogPane() {
		initComponents();
		// Find out which aggregation functions are loaded.
		Collection<String> list = new ArrayList<>();
		Iterator<FunctionDescription> it = AggregationRegistry.getInstance().getFunctionDescriptions().iterator();
		while(it.hasNext()) {
			FunctionDescription fd = it.next();
			String desc = StringUtils.rightPad(fd.getName(), 20) + fd.getDescription();
			list.add(desc);
		}
		fcomboBox.setModel(new DefaultComboBoxModel<String>(list.toArray(new String[]{})));

		
	}
	
	@Override
	public boolean isMetadataRequiredForConfiguration(int portIndex) {
		return true;
	}

	@Override
	public Component getComponent() {
		return this;
	}
	
	private void initComponents() {
		this.expression = new JTextArea(10, 20);
		expression.setWrapStyleWord(true);
		expression.setFont(new Font("Monospaced", Font.PLAIN, 11));
		
		this.expressionPanel = new JScrollPane(expression);
		this.expressionPanel.setBorder(BorderFactory.createTitledBorder("Group Expressions"));
		
		functionPanel = new JPanel();
		functionPanel.setLayout(new GridLayout(0, 1, 0, 0));
		functionPanel.setBorder(BorderFactory.createTitledBorder("Available Aggregate Functions"));
		
		fcomboBox = new JComboBox();
		fcomboBox.setMaximumRowCount(10);
		fcomboBox.setModel(new DefaultComboBoxModel(new String[] {"Function - Description"}));
		fcomboBox.setFont(new Font("Monospaced", Font.PLAIN, 10));
		functionPanel.add(fcomboBox);
		
		GroupLayout groupLayout = new GroupLayout(this);
		groupLayout.setHorizontalGroup(
			groupLayout.createParallelGroup(Alignment.TRAILING)
				.addGroup(groupLayout.createSequentialGroup()
					.addGroup(groupLayout.createParallelGroup(Alignment.TRAILING)
						.addGroup(Alignment.LEADING, groupLayout.createSequentialGroup()
							.addContainerGap()
							.addComponent(columnSelect, GroupLayout.DEFAULT_SIZE, 433, Short.MAX_VALUE))
						.addGroup(Alignment.LEADING, groupLayout.createSequentialGroup()
							.addGap(7)
							.addComponent(expressionPanel, GroupLayout.DEFAULT_SIZE, 436, Short.MAX_VALUE))
						.addGroup(Alignment.LEADING, groupLayout.createSequentialGroup()
							.addContainerGap()
							.addComponent(functionPanel, GroupLayout.DEFAULT_SIZE, 433, Short.MAX_VALUE)))
					.addGap(7))
		);
		groupLayout.setVerticalGroup(
			groupLayout.createParallelGroup(Alignment.LEADING)
				.addGroup(groupLayout.createSequentialGroup()
					.addGap(5)
					.addComponent(expressionPanel, GroupLayout.DEFAULT_SIZE, 203, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(functionPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(columnSelect, GroupLayout.PREFERRED_SIZE, 248, GroupLayout.PREFERRED_SIZE)
					.addContainerGap())
		);
		setLayout(groupLayout);
	}

	@Override
	public AbstractDRSettingsModel<Group> getSettings() {
		return settings;
	}

	@Override
	public void refresh(PortMetadata[] specs) {
		this.inType = ((RecordMetadata)specs[0]).getType();
	    
	    List<String> inputFields = Arrays.asList(this.inType.getNames());
	    List<String> currentKeyFields = new ArrayList<>();
	    currentKeyFields.addAll(Arrays.asList(this.settings.keys.getStringArrayValue()));
	    List<String> nonKeyFields = new ArrayList<>();
	    nonKeyFields.addAll(inputFields);
	    nonKeyFields.removeAll(currentKeyFields);
	    
	    currentKeyFields.retainAll(inputFields);
	    this.columnSelect.setExcludeList(nonKeyFields);
	    this.columnSelect.setIncludeList(currentKeyFields);
	    this.expression.setText(this.settings.expression.getStringValue());
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		this.settings.keys.setStringArrayValue(this.columnSelect.getIncludeList().toArray(new String[]{}));
		this.settings.expression.setStringValue(this.expression.getText());
	}
}
