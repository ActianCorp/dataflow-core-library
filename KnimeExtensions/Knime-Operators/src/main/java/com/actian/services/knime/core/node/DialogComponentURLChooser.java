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
package com.actian.services.knime.core.node;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.GroupLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;

import com.pervasive.datarush.knime.io.FileBrowserUtil;

public final class DialogComponentURLChooser extends DialogComponent {

	private JButton browseButton;
	private JComboBox<String> filepathText;
	private JLabel locationLabel;
	private JCheckBox readLocalCheckbox;
	
	private final StringHistory history;
	
    /**
     * Provides a dialog control which can invoke a file browser with knowledge
     * of remote file systems specified in the Actian Preferences.
     * @param model the {@link SettingsModelURLString} to store.
     */
	public DialogComponentURLChooser(SettingsModel model) {
		this(model,"drFileDialogHistoryID");
	}
	
    /**
     * Provides a dialog control which can invoke a file browser with knowledge
     * of remote file systems specified in the Actian Preferences.
     * 
     * @param model the {@link SettingsModelURLString} to store
     * @param historyName family name of URLs to retrieve/store in KNIME
     */
	public DialogComponentURLChooser(SettingsModel model, String historyName) {
		super(model);
		
		initComponents();
		
		this.history = StringHistory.getInstance(historyName);
		for (String s : this.history.getHistory()) {
			this.filepathText.addItem(s);
		}
		this.browseButton.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				FileBrowserUtil.browseFiles(DialogComponentURLChooser.this.filepathText,0,2);
				
			}
		});
		
	}

	private void initComponents() {
		this.locationLabel = new JLabel("Location:");
		this.readLocalCheckbox = new JCheckBox(
				"Read file locally instead of in cluster");
		this.filepathText = new JComboBox<String>();
		this.browseButton = new JButton();

		this.filepathText.setEditable(true);

		this.browseButton.setText("Browse...");

		JPanel p = new JPanel();
		GroupLayout layout = new GroupLayout(p);
		
        p.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addComponent(locationLabel)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(filepathText, javax.swing.GroupLayout.PREFERRED_SIZE, 512, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(browseButton))
                    .addComponent(readLocalCheckbox)))
        );
        
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(locationLabel)
                    .addComponent(filepathText, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(browseButton))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(readLocalCheckbox))
        );
        
        this.getComponentPanel().add(p);
	}
	 
	@Override
	protected void updateComponent() {
		SettingsModelURLString model = (SettingsModelURLString) this.getModel();
		String newValue = model.getStringValue();
		
		boolean update;
		
		if (newValue == null) {
			update = this.filepathText.getSelectedItem() != null;
		}
		else {
			String file = this.filepathText.getEditor().getItem().toString();
			update = !newValue.equals(file);
		}
		
		model.setLocalRead(this.readLocalCheckbox.isSelected());
		
		if (update) {
			this.filepathText.removeItem(newValue);
			this.filepathText.addItem(newValue);
			this.filepathText.setSelectedItem(newValue);
		}
		setEnabledComponents(model.isEnabled());
	}

	@Override
	protected void validateSettingsBeforeSave() throws InvalidSettingsException {
		
		SettingsModelURLString model = (SettingsModelURLString) this.getModel();
		Object selected = this.filepathText.getEditor().getItem();
		
		model.setFileName(selected != null ? selected.toString() : null);
	}

	@Override
	protected void checkConfigurabilityBeforeLoad(PortObjectSpec[] specs)
			throws NotConfigurableException {
	}

	@Override
	protected void setEnabledComponents(boolean enabled) {
		this.browseButton.setEnabled(enabled);
		this.filepathText.setEnabled(enabled);
		this.readLocalCheckbox.setEnabled(enabled);
	}

	@Override
	public void setToolTipText(String text) {
		this.browseButton.setToolTipText(text);
		this.filepathText.setToolTipText(text);
	}

}
