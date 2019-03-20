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
		assertEquals(6, list.size());
	}
		
	@Test
	public void testDaoCheckCellProfiles() throws DaoException {

		ArrayList<CellProfile> list = DaoCellProfile.getAllCellProfiles(studyId);
		CellProfile cellProfile = list.get(0);
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Putative copy-number alterations from GISTIC", cellProfile.getProfileName());
		assertEquals(CellAlterationType.COPY_NUMBER_ALTERATION, cellProfile.getCellAlterationType());
		assertEquals("Putative copy-number from GISTIC 2.0. Values: -2 = homozygous deletion; -1 = hemizygous deletion; 0 = neutral / no change; 1 = gain; 2 = high level amplification.", 
				cellProfile.getProfileDescription());

		cellProfile = list.get(1);
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("mRNA expression (microarray)", cellProfile.getProfileName());
		assertEquals(CellAlterationType.MRNA_EXPRESSION, cellProfile.getCellAlterationType());
		assertEquals(false, cellProfile.showProfileInAnalysisTab());
	}
	
	@Test
	public void testDaoCreateCellProfile() throws DaoException {

		CellProfile cellProfile = new CellProfile();
		cellProfile.setCancerStudyId(studyId);
		cellProfile.setProfileName("test profile");
		cellProfile.setStableId("test");
		cellProfile.setCellAlterationType(CellAlterationType.FUSION);
		cellProfile.setDatatype("test");
		DaoCellProfile.addCellProfile(cellProfile);
		
		CellProfile readCellProfile = DaoCellProfile.getCellProfileByStableId("test");
		assertEquals(studyId, readCellProfile.getCancerStudyId());
		assertEquals("test", readCellProfile.getStableId());
		assertEquals("test profile", readCellProfile.getProfileName());
		assertEquals(CellAlterationType.FUSION, readCellProfile.getCellAlterationType());
	}

	@Test
	public void testDaoGetCellProfileByStableId() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId("study_tcga_pub_gistic");
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Putative copy-number alterations from GISTIC", cellProfile.getProfileName());
		assertEquals(CellAlterationType.COPY_NUMBER_ALTERATION, cellProfile.getCellAlterationType());
	}
	
	@Test
	public void testDaoGetCellProfileByInternalId() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileById(2);
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Putative copy-number alterations from GISTIC", cellProfile.getProfileName());
		assertEquals(CellAlterationType.COPY_NUMBER_ALTERATION, cellProfile.getCellAlterationType());
	}
	
	@Test
	public void testDaoDeleteCellProfile() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileById(2);

		assertEquals(6, DaoCellProfile.getCount());
		DaoCellProfile.deleteCellProfile(cellProfile);
		assertEquals(5, DaoCellProfile.getCount());
		
		ArrayList<CellProfile> list = DaoCellProfile.getAllCellProfiles(studyId);
		assertEquals(5, list.size());
		cellProfile = list.get(0);
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("mRNA expression (microarray)", cellProfile.getProfileName());
		assertEquals(CellAlterationType.MRNA_EXPRESSION, cellProfile.getCellAlterationType());
	}

	@Test
	public void testDaoUpdateCellProfile() throws DaoException {

		CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId("study_tcga_pub_gistic");

		assertTrue(DaoCellProfile.updateNameAndDescription(
				cellProfile.getCellProfileId(), "Updated Name",
				"Updated Description"));
		ArrayList<CellProfile> list = DaoCellProfile.getAllCellProfiles(studyId);
		assertEquals(6, list.size());
		cellProfile = list.get(0);
		assertEquals(studyId, cellProfile.getCancerStudyId());
		assertEquals("Updated Name", cellProfile.getProfileName());
		assertEquals(CellAlterationType.COPY_NUMBER_ALTERATION, cellProfile.getCellAlterationType());
		assertEquals("Updated Description", cellProfile.getProfileDescription());
	}
}
