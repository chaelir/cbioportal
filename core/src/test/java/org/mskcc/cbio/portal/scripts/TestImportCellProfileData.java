/*
 * Copyright (c) 2016 The Hyve B.V.
 * This code is licensed under the GNU Affero Cellral Public License (AGPL),
 * version 3, or (at your option) any later version.
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

package org.mskcc.cbio.portal.scripts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoClinicalAttributeMeta;
import org.mskcc.cbio.portal.dao.DaoClinicalData;
import org.mskcc.cbio.portal.dao.DaoCnaEvent;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoCellOptimized;
import org.mskcc.cbio.portal.dao.DaoCellProfile;
import org.mskcc.cbio.portal.dao.DaoMutation;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.dao.DaoSampleProfile;
import org.mskcc.cbio.portal.dao.MySQLbulkLoader;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CanonicalCell;
import org.mskcc.cbio.portal.model.ClinicalAttribute;
import org.mskcc.cbio.portal.model.ClinicalData;
import org.mskcc.cbio.portal.model.CnaEvent;
import org.mskcc.cbio.portal.model.ExtendedMutation;
import org.mskcc.cbio.portal.model.CellProfile;
import org.mskcc.cbio.portal.model.Sample;

import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.ImportDataUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Pieter Lukasse pieter@thehyve.nl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
public class TestImportCellProfileData {

    int studyId;
    int cellProfileId;

    @Before
    public void setUp() throws DaoException {
        ProgressMonitor.setConsoleMode(false);
        loadCells();
    }

    @After
    public void cleanUp() throws DaoException {
        // each test assumes the mutation data hasn't been loaded yet
        CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId("linear_CRA");
        if (cellProfile != null) {
            DaoCellProfile.deleteCellProfile(cellProfile);
            assertNull(DaoCellProfile.getCellProfileByStableId("linear_CRA"));
        }
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /* NOTE: disabled mutation stuff
    @Test
    public void testImportMutationsFile() throws Exception {
        /*
         * Complex test where we import a mutations file split over two data
         * files. The data includes germline mutations as well as silent
         * mutations. We make sure the nonsynonymous somatic and germline
         * mutations are added to the databases and the MUTATION_COUNT clinical
         * attributes are correctly computed.
        String[] args = {
                "--data","src/test/resources/data_mutations_extended.txt",
                "--meta","src/test/resources/meta_mutations_extended.txt",
                "--loadMode", "bulkLoad"
        };
        ImportProfileData runner = new ImportProfileData(args);
        runner.run();

        // check the study exists
        String studyStableId = "study_tcga_pub";
        CancerStudy study = DaoCancerStudy.getCancerStudyByStableId(studyStableId);
        assertNotNull(study);
        studyId = study.getInternalId();

        // Check if the ImportProfileData class indeed adds the study stable Id in front of the
        //dataset study id (e.g. studyStableId + "_breast_mutations"):
        CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId(studyStableId + "_breast_mutations");
        assertNotNull(cellProfile);
        cellProfileId = cellProfile.getCellProfileId();

        // check the mutation T433A has been imported
        int sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-AA-3664-01").getInternalId();
        validateMutationAminoAcid(cellProfileId, sampleId, 54407, "T433A");

        // data for the second sample should not exist before loading the next data file
        int secondSampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-AA-3665-01").getInternalId();
        assertEquals(DaoMutation.getMutations(cellProfileId, secondSampleId).size(), 0);
        // the CELL_PROFILE_ID in sample_profile should be the same as the
        // cell profile that was used for import
        int cellProfileIdFromSampleProfile = DaoSampleProfile.getProfileIdForSample(sampleId);
        assertEquals(cellProfileIdFromSampleProfile, cellProfileId);

        // assume clinical data for MUTATION_COUNT was created
        ClinicalAttribute clinicalAttribute = DaoClinicalAttributeMeta.getDatum("MUTATION_COUNT", studyId);
        assertNotNull(clinicalAttribute);

        // assume a MUTATION_COUNT record has been added for the sample and the
        // count is 8 there 11 total mutations imported of which 3 germline (
        // not entirely sure why the rest doesn't get imported i see some silent
        // + intron, missing entrez id)
        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(study.getInternalId(), new ArrayList<String>(Arrays.asList("TCGA-AA-3664-01")), clinicalAttribute);
        assert(clinicalData.size() == 1);
        assertEquals("8", clinicalData.get(0).getAttrVal());

        // load a second mutation data file
        String[] secondArgs = {
                "--data","src/test/resources/data_mutations_extended_continued.txt",
                "--meta","src/test/resources/meta_mutations_extended.txt",
                "--loadMode", "bulkLoad"
        };
        ImportProfileData secondRunner = new ImportProfileData(secondArgs);
        secondRunner.run();

        // check mutation for second sample was imported
        validateMutationAminoAcid(cellProfileId, secondSampleId, 2842, "L113P");

        // assume a MUTATION_COUNT record has been added for the sample and the
        // count is 1, the other one is a germline mutation
        // also confirm mutation count for first sample is still correct
        clinicalData = DaoClinicalData.getSampleData(study.getInternalId(), new ArrayList<String>(Arrays.asList("TCGA-AA-3664-01", "TCGA-AA-3665-01")), clinicalAttribute);
        assert(clinicalData.size() == 2);
        assertEquals("8", clinicalData.get(0).getAttrVal());
        assertEquals("1", clinicalData.get(1).getAttrVal());
    }
    */

    /* NOTE: disabled mutation stuff
    @Test
    public void testImportSplitMutationsFile() throws Exception {
        /*
         * Mutations file split over two files with same stable id. Make sure
         * that the first time if a sample is in the #sequenced_samples the
         * MUTATION_COUNT is 0. After importing the second file make sure the
         * counts are added up i.e. mutations from both the first and second
         * file should be included in the MUTATION_COUNT record.
        String[] args = {
                "--data","src/test/resources/splitMutationsData/data_mutations_extended.txt",
                "--meta","src/test/resources/splitMutationsData/meta_mutations_extended.txt",
                "--loadMode", "bulkLoad"
        };
        ImportProfileData runner = new ImportProfileData(args);
        runner.run();
        String studyStableId = "study_tcga_pub";
        CancerStudy study = DaoCancerStudy.getCancerStudyByStableId(studyStableId);
        studyId = study.getInternalId();

        // assume clinical data for MUTATION_COUNT was created
        ClinicalAttribute clinicalAttribute = DaoClinicalAttributeMeta.getDatum("MUTATION_COUNT", studyId);
        assertNotNull(clinicalAttribute);

        // assume a MUTATION_COUNT record has been added for both samples
        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(study.getInternalId(), new ArrayList<String>(Arrays.asList("TCGA-AA-3664-01", "TCGA-AA-3665-01")), clinicalAttribute);
        assert(clinicalData.size() == 2);
        assertEquals("3", clinicalData.get(0).getAttrVal());
        assertEquals("0", clinicalData.get(1).getAttrVal());

        // load a second mutation data file
        String[] secondArgs = {
                "--data","src/test/resources/splitMutationsData/data_mutations_extended_continued.txt",
                "--meta","src/test/resources/splitMutationsData/meta_mutations_extended.txt",
                "--loadMode", "bulkLoad"
        };
        ImportProfileData secondRunner = new ImportProfileData(secondArgs);
        secondRunner.run();

        // assume a MUTATION_COUNT record has been updated for both samples (both +1)
        clinicalData = DaoClinicalData.getSampleData(study.getInternalId(), new ArrayList<String>(Arrays.asList("TCGA-AA-3664-01", "TCGA-AA-3665-01")), clinicalAttribute);
        assert(clinicalData.size() == 2);
        assertEquals("4", clinicalData.get(0).getAttrVal());
        assertEquals("1", clinicalData.get(1).getAttrVal());
    }
    */

    /* NOTE: disabled mutation stuff
    @Test
    public void testImportGermlineOnlyFile() throws Exception {
        // Mutations file split over two files with same stable id
        String[] args = {
                "--data","src/test/resources/germlineOnlyMutationsData/data_mutations_extended.txt",
                "--meta","src/test/resources/germlineOnlyMutationsData/meta_mutations_extended.txt",
                "--loadMode", "bulkLoad"
        };
        ImportProfileData runner = new ImportProfileData(args);
        runner.run();
        String studyStableId = "study_tcga_pub";
        CancerStudy study = DaoCancerStudy.getCancerStudyByStableId(studyStableId);
        studyId = study.getInternalId();
        int sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-AA-3664-01").getInternalId();
        CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId(studyStableId + "_breast_mutations");
        cellProfileId = cellProfile.getCellProfileId();

        // assume clinical data for MUTATION_COUNT was created
        ClinicalAttribute clinicalAttribute = DaoClinicalAttributeMeta.getDatum("MUTATION_COUNT", studyId);
        assertNotNull(clinicalAttribute);

        // assume a MUTATION_COUNT record has been added for one sample and the count is zero
        List<ClinicalData> clinicalData = DaoClinicalData.getSampleData(study.getInternalId(), new ArrayList<String>(Arrays.asList("TCGA-AA-3664-01")), clinicalAttribute);
        assert(clinicalData.size() == 1);
        assertEquals("0", clinicalData.get(0).getAttrVal());

        // check if the three germline mutations have been inserted
        validateMutationAminoAcid (cellProfileId, sampleId, 64581, "T209A");
        validateMutationAminoAcid (cellProfileId, sampleId, 50839, "G78S");
        validateMutationAminoAcid (cellProfileId, sampleId, 2842, "L113P");

        // remove profile at the end
        DaoCellProfile.deleteCellProfile(cellProfile);
        assertNull(DaoCellProfile.getCellProfileByStableId(studyStableId + "_breast_mutations"));
    }

    @Test
    public void testImportCNAFile() throws Exception {
        //cells in this test:
        DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
        daoCell.addCell(new CanonicalCell(999999672, "TESTBRCA1"));
        daoCell.addCell(new CanonicalCell(999999675, "TESTBRCA2"));
        MySQLbulkLoader.flushAll();
        String[] args = {
                "--data","src/test/resources/data_CNA_sample.txt",
                "--meta","src/test/resources/meta_CNA.txt" ,
                "--noprogress",
                "--loadMode", "bulkLoad"
        };
        String[] sampleIds = {"TCGA-02-0001-01","TCGA-02-0003-01","TCGA-02-0004-01","TCGA-02-0006-01"};
        //This test is to check if the ImportProfileData class indeed adds the study stable Id in front of the
        //dataset study id (e.g. studyStableId + "_breast_mutations"):
        String studyStableId = "study_tcga_pub";
        CancerStudy study = DaoCancerStudy.getCancerStudyByStableId(studyStableId);
        studyId = study.getInternalId();
        //will be needed when relational constraints are active:
        ImportDataUtil.addPatients(sampleIds, study);
        ImportDataUtil.addSamples(sampleIds, study);
        try {
            ImportProfileData runner = new ImportProfileData(args);
            runner.run();
        } catch (Throwable e) {
            //useful info for when this fails:
            ConsoleUtil.showMessages();
            throw e;
        }
        cellProfileId = DaoCellProfile.getCellProfileByStableId(studyStableId + "_cna").getCellProfileId();
        List<Integer> sampleInternalIds = new ArrayList<Integer>();
        DaoSample.reCache();
        for (String sample : sampleIds) {
            sampleInternalIds.add(DaoSample.getSampleByCancerStudyAndSampleId(studyId, sample).getInternalId());
        }
        Collection<Short> cnaLevels = Arrays.asList((short)-2, (short)2);
        List<CnaEvent> cnaEvents = DaoCnaEvent.getCnaEvents(sampleInternalIds, null, cellProfileId, cnaLevels);
        assertEquals(2, cnaEvents.size());
        //validate specific records. Data looks like:
        //999999672    TESTBRCA1    -2    0    1    0
        //999999675    TESTBRCA2    0    2    0    -1
        //Check if the first two samples are loaded correctly:
        int sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-02-0001-01").getInternalId();
        sampleInternalIds = Arrays.asList((int)sampleId);
        CnaEvent cnaEvent = DaoCnaEvent.getCnaEvents(sampleInternalIds, null, cellProfileId, cnaLevels).get(0);
        assertEquals(-2, cnaEvent.getAlteration().getCode());
        assertEquals("TESTBRCA1", cnaEvent.getCellSymbol());
        sampleId = DaoSample.getSampleByCancerStudyAndSampleId(studyId, "TCGA-02-0003-01").getInternalId();
        sampleInternalIds = Arrays.asList((int)sampleId);
        cnaEvent = DaoCnaEvent.getCnaEvents(sampleInternalIds, null, cellProfileId, cnaLevels).get(0);
        assertEquals(2, cnaEvent.getAlteration().getCode());
        assertEquals("TESTBRCA2", cnaEvent.getCellSymbol());
    }

    private void validateMutationAminoAcid (int cellProfileId, Integer sampleId, long entrezCellId, String expectedAminoAcidChange) throws DaoException {
        ArrayList<ExtendedMutation> mutationList = DaoMutation.getMutations(cellProfileId, sampleId, entrezCellId);
        assertEquals(1, mutationList.size());
        assertEquals(expectedAminoAcidChange, mutationList.get(0).getProteinChange());
    }
    */

    private static void loadCells() throws DaoException {
        DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
        // add the 5 cells used in "data_linear_CRA.txt"
        daoCell.addCell(new CanonicalCell(1, "B_CELL"));
        daoCell.addCell(new CanonicalCell(2, "MEMORY_B_CELL"));
        daoCell.addCell(new CanonicalCell(3, "ACTIVATED_B_CELL"));
        daoCell.addCell(new CanonicalCell(4, "NAIVE_B_CELL"));
        daoCell.addCell(new CanonicalCell(5, "BASOPHIL"));
        
        MySQLbulkLoader.flushAll();
    }
}
