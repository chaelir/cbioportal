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
 * You should have received a copy of the GNU GPL
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.model.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * JUnit test for DaoSample class
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
public class TestDaoSampleCellProfile {

	CancerStudy study;
	ArrayList<Integer> internalSampleIds;
	int cellProfileId;
	
	@Before
	public void setUp() throws DaoException {
		study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
		cellProfileId = DaoCellProfile.getCellProfileByStableId("linear_CRA").getCellProfileId();
		
		internalSampleIds = new ArrayList<Integer>();
        Patient p = new Patient(study, "TCGA-54321");
        int pId = DaoPatient.addPatient(p);
        
        DaoSample.reCache();
        Sample s = new Sample("TCGA-54321-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("TCGA-09876-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
	}

	@Test
    public void testDaoSampleCellProfile() throws DaoException {

        Patient patient = DaoPatient.getPatientByCancerStudyAndPatientId(study.getInternalId(), "TCGA-54321");
        Sample sample = DaoSample.getSampleByPatientAndSampleId(patient.getInternalId(), "TCGA-54321-01");

        int num = DaoSampleCellProfile.addSampleCellProfile(sample.getInternalId(), cellProfileId, null);
        assertEquals(1, num);

        boolean exists = DaoSampleCellProfile.sampleExistsInCellProfile(sample.getInternalId(), cellProfileId);
        assertTrue(exists);

        assertEquals(cellProfileId, DaoSampleCellProfile.getCellProfileIdForSample(sample.getInternalId()));

        sample = DaoSample.getSampleByPatientAndSampleId(patient.getInternalId(), "TCGA-09876-01");
        num = DaoSampleCellProfile.addSampleCellProfile(sample.getInternalId(), cellProfileId, null);
        assertEquals(1, num);

        /* NOTE:
           expected: 6 existing samples in linear_CR + the 2 just added samples
        */
        ArrayList<Integer> sampleIds = DaoSampleCellProfile.getAllSampleIdsInProfile(cellProfileId);
        assertEquals(8, sampleIds.size());
    }

}
