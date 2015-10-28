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

import org.knime.core.node.defaultnodesettings.SettingsModelString;

public class SettingsModelURLString extends SettingsModelString {

	private boolean localRead=true;
	
	/**
	 * Creates a new object holding a URL value.
	 * @param configName the identifier the value is stored with in the
	 * 				{@link org.knime.core.node.NodeSettings} object
	 * @param defaultValue the initial value
	 */
	public SettingsModelURLString(String configName, String defaultValue) {
		super(configName, defaultValue);
	}

	/**
	 * 
	 * @return the current URL stored.
	 */
	public String getFileName() {
		return this.getStringValue();
	}

    /**
     * {@inheritDoc }
     */
	public boolean getIncludeSourceInfo() {
		return false;
	}

    /**
     * 
     * @return true if the URL is read locally rather than from the cluster 
     */
	public boolean getLocalRead() {
		return localRead;
	}

	public void setFileName(String fileName) {
		this.setStringValue(fileName);
	}

	public void setIncludeSourceInfo(boolean value) {
		return;
	}

	public void setLocalRead(boolean arg0) {
		this.localRead=true;
	}

}
