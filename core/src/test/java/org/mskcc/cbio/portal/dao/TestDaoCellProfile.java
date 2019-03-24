/*
 * Copyright (c) 2015 - 2016 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
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

package org.mskcc.cbio.portal.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.model.CellAlterationType;
import org.mskcc.cbio.portal.model.CellProfile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * JUnit tests for DaoCellProfile class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
public class TestDaoCellProfile {
	
	int studyId;
	
	@Before 
	public void setUp() throws DaoException
	{
		studyId = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub").getInternalId();
		DaoCellProfile.reCache();
	}

	@Test
	public void testDaoGetAllCellProfiles() throws DaoException {

		ArrayList<CellProfile> list = DaoCellProfile.getAllCellProfiles(studyId);
		assertEquals(2, list.size()); // two cell profiles in cgds_test
	}
		
	@Test
	public void testDaoCheckCellProfiles() throws DaoException {

		ArrayList<CellProfile> list = DaoCellProfile.getAllCellProfiles(studyId);
		CellProfile cellProfile = list.get(0); //first entry
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Relative immune cell abundance values from CiberSort", cellProfile.getProfileName());
		assertEquals(CellAlterationType.CELL_RELATIVE_ABUNDANCE, cellProfile.getCellAlterationType());
		assertEquals("Relative linear relative abundance values (0 to 1) for each cell type", 
				cellProfile.getProfileDescription());

	}

    @Test
	public void testDaoCreateCellProfile() throws DaoException {

		CellProfile cellProfile = new CellProfile();
		cellProfile.setCancerStudyId(studyId);
		cellProfile.setProfileName("test profile");
		cellProfile.setStableId("test");
		cellProfile.setCellAlterationType(CellAlterationType.CELL_RELATIVE_ABUNDANCE);
		cellProfile.setDatatype("test");
		DaoCellProfile.addCellProfile(cellProfile);
		
		CellProfile readCellProfile = DaoCellProfile.getCellProfileByStableId("test");
		assertEquals(studyId, readCellProfile.getCancerStudyId());
		assertEquals("test", readCellProfile.getStableId());
		assertEquals("test profile", readCellProfile.getProfileName());
		assertEquals(CellAlterationType.CELL_RELATIVE_ABUNDANCE, readCellProfile.getCellAlterationType());
	}

	@Test
	public void testDaoGetCellProfileByStableId() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId("linear_CRA");
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Relative immune cell abundance values from CiberSort", cellProfile.getProfileName());
		assertEquals(CellAlterationType.CELL_RELATIVE_ABUNDANCE, cellProfile.getCellAlterationType());
	}
	
	@Test
	public void testDaoGetCellProfileByInternalId() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileById(1);
		assertEquals(studyId, cellProfile.getCancerStudyId());
        assertEquals("Relative immune cell abundance values from CiberSort", cellProfile.getProfileName());
        assertEquals(CellAlterationType.CELL_RELATIVE_ABUNDANCE, cellProfile.getCellAlterationType());
	}
	
	@Test
	public void testDaoDeleteCellProfile() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileById(1);

		assertEquals(2, DaoCellProfile.getCount()); // two cell profiles in test data
		DaoCellProfile.deleteCellProfile(cellProfile);
		assertEquals(1, DaoCellProfile.getCount());
		
	}

	@Test
	public void testDaoUpdateCellProfile() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId("linear_CRA");

		assertTrue(DaoCellProfile.updateNameAndDescription(
				cellProfile.getCellProfileId(), "Updated Name",
				"Updated Description"));
		ArrayList<CellProfile> list = DaoCellProfile.getAllCellProfiles(studyId);
		assertEquals(2, list.size()); //linear_CRA and linear_CRA_test
		cellProfile = list.get(0);
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Updated Name", cellProfile.getProfileName());
		assertEquals(CellAlterationType.CELL_RELATIVE_ABUNDANCE, cellProfile.getCellAlterationType());
		assertEquals("Updated Description", cellProfile.getProfileDescription());
	}
}
