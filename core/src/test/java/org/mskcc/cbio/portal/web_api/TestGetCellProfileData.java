/*
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
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
 * it under the terms of the GNU Affero Cellral Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero Cellral Public License for more details.
 *
 * You should have received a copy of the GNU Affero Cellral Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.web_api;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * JUnit test for GetCellProfileData class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
public class TestGetCellProfileData {
	
	int cellProfileId;
	
	@Before
	public void setUp() throws DaoException {
		int studyId = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub").getInternalId();
		
		DaoPatient.reCache();
		DaoSample.reCache();
		DaoCellProfile.reCache();
		
		CellProfile newCellProfile = new CellProfile();
		newCellProfile.setCancerStudyId(studyId);
		newCellProfile.setCellAlterationType(CellAlterationType.CELL_RELATIVE_ABUNDANCE);
		newCellProfile.setStableId("study_tcga_pub_test");
		newCellProfile.setProfileName("Barry CiberSort Results");
		newCellProfile.setDatatype("test");
		DaoCellProfile.addCellProfile(newCellProfile);
		
		cellProfileId =  DaoCellProfile.getCellProfileByStableId("study_tcga_pub_test").getCellProfileId();
	}

    @Test
    public void testGetCellProfileData() throws DaoException, IOException {
        DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
        // add the 5 cells used in "data_linear_CRA.txt"
        daoCell.addCell(new CanonicalCell(1, "B_CELL"));
        daoCell.addCell(new CanonicalCell(2, "MEMORY_B_CELL"));
        daoCell.addCell(new CanonicalCell(3, "ACTIVATED_B_CELL"));
        daoCell.addCell(new CanonicalCell(4, "NAIVE_B_CELL"));
        daoCell.addCell(new CanonicalCell(5, "BASOPHIL"));

        ArrayList <String> targetCellList = new ArrayList<String> ();
        targetCellList.add("B_CELL");
        targetCellList.add("MEMORY_B_CELL");
        targetCellList.add("ACTIVATED_B_CELL");
        targetCellList.add("NAIVE_B_CELL");
        targetCellList.add("BASOPHIL");

        ArrayList <String> cellProfileIdList = new ArrayList<String>();
        cellProfileIdList.add("linear_CRA");

        ArrayList <String> sampleIdList = new ArrayList <String>();
        sampleIdList.add("TCGA-A1-A0SB-01");
        sampleIdList.add("TCGA-A1-A0SD-01");
        sampleIdList.add("TCGA-A1-A0SE-01");

        GetCellProfileData getCellProfileData = new GetCellProfileData(cellProfileIdList, targetCellList,
                sampleIdList, new Boolean(false));
        String out = getCellProfileData.getRawContent();
        String lines[] = out.split("\n");
        assertEquals("# DATA_TYPE\t Relative immune cell abundance values from CiberSort" , lines[0]);
        assertEquals("# COLOR_GRADIENT_SETTINGS\t CELL_RELATIVE_ABUNDANCE", lines[1]);
        assertTrue(lines[2].startsWith("GENE_ID\tCOMMON\tTCGA-A1-A0SB-01\t" +
                "TCGA-A1-A0SD-01\tTCGA-A1-A0SE-01"));
        assertTrue(lines[3].startsWith("1\tB_CELL\t0\t0\t0"));
        assertTrue(lines[4].startsWith("2\tMEMORY_B_CELL\t0\t0\t0"));
    }
}
