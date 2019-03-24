/*
 * Copyright (c) 2017 The Hyve B.V.
 * This code is licensed under the GNU Affero General Public License (AGPL),
 * version 3, or (at your option) any later version.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
 * @author Sander Tan
*/

package org.mskcc.cbio.portal.model;

import java.io.Serializable;

public class CellProfileLink implements Serializable {
	
	private int referringCellProfileId;
	private int referredCellProfileId;
	private String referenceType;
	
	public int getReferringCellProfileId() {
		return referringCellProfileId;
	}
	public void setReferringCellProfileId(int referringCellProfileId) {
		this.referringCellProfileId = referringCellProfileId;
	}
	public int getReferredCellProfileId() {
		return referredCellProfileId;
	}
	public void setReferredCellProfileId(int referredCellProfileId) {
		this.referredCellProfileId = referredCellProfileId;
	}
	public String getReferenceType() {
		return referenceType;
	}
	public void setReferenceType(String referenceType) {
		this.referenceType = referenceType;
	}
}
