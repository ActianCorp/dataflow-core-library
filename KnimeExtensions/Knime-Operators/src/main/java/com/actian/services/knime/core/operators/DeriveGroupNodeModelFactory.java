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

import com.pervasive.datarush.knime.core.framework.AbstractDRNodeFactory;
import com.pervasive.datarush.knime.core.framework.DRNodeModel;
import com.pervasive.datarush.knime.coreui.common.CustomDRNodeDialogPane;
import com.pervasive.datarush.operators.group.Group;

public class DeriveGroupNodeModelFactory extends AbstractDRNodeFactory<Group> {

	@Override
	protected DRNodeModel<Group> createDRNodeModel() {
		return new DRNodeModel<Group>( new Group(), new DeriveGroupNodeSettings());
	}

	@Override
	protected CustomDRNodeDialogPane<Group> createNodeDialogPane() {
		CustomDRNodeDialogPane<Group> dialog = new CustomDRNodeDialogPane<Group>(new Group(), new DeriveGroupNodeDialogPane());
		dialog.setDefaultTabTitle("Properties");
		return dialog;
	}

	@Override
	protected boolean hasDialog() {
		return true;
	}

}
