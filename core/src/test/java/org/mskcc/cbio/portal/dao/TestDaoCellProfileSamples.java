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

package org.mskcc.cbio.portal.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.Patient;
import org.mskcc.cbio.portal.model.Sample;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * JUnit Tests for the Dao Cell Profile Cases Class.
 *
 * @author Ethan Cerami.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
public class TestDaoCellProfileSamples {
	
	CancerStudy study;
	ArrayList<Integer> internalSampleIds;
	int cellProfileId;
	
	@Before
	public void setUp() throws DaoException {
		study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
		cellProfileId = DaoCellProfile.getCellProfileByStableId("linear_CRA_test").getCellProfileId();
		
		internalSampleIds = new ArrayList<Integer>();
        Patient p = new Patient(study, "CELL-A2"); //?use the same id as TestDaoCellAlteration
        int pId = DaoPatient.addPatient(p);
        
        DaoSample.reCache();
        Sample s = new Sample("CELL-A2-TEST-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("CELL-A2-TEST-02", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("CELL-A2-TEST-03", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("CELL-A2-TEST-04", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
	}

    /**
     * Tests the Dao Cell Profile Samples Class.
     * @throws DaoException Database Exception.
     */
	@Test
    public void testDaoCellProfileSamples() throws DaoException {

        ArrayList<Integer> orderedSampleList = new ArrayList<Integer>();
        int numRows = DaoCellProfileSamples.addCellProfileSamples(cellProfileId, internalSampleIds);

        assertEquals (1, numRows);

        orderedSampleList = DaoCellProfileSamples.getOrderedSampleList(cellProfileId);
        assertEquals (4, orderedSampleList.size());

        //  Test the Delete method
        DaoCellProfileSamples.deleteAllSamplesInCellProfile(cellProfileId);
        orderedSampleList = DaoCellProfileSamples.getOrderedSampleList(cellProfileId);
        assertEquals (0, orderedSampleList.size());
    }

}