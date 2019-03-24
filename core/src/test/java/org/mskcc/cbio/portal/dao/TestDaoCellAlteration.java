/* LICENSE_TBD */

package org.mskcc.cbio.portal.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.model.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.*;

import java.util.*;

/**
 * JUnit tests for DaoCellAlteration class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
public class TestDaoCellAlteration {
	
	CancerStudy study;
	ArrayList<Integer> internalSampleIds;
	int cellProfileId;

	// I am modifying this test to operate on an empty cell profile
	
	@Before
	public void setUp() throws DaoException {
		study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
		cellProfileId = DaoCellProfile.getCellProfileByStableId("linear_CRA_test").getCellProfileId();
		// get cellProfileId of existing cell profile with stable_id linear_CRA
		
		internalSampleIds = new ArrayList<Integer>();
        Patient p = new Patient(study, "CELL-A1");
        int pId = DaoPatient.addPatient(p);
        
        DaoSample.reCache();
        Sample s = new Sample("CELL-A1-TEST-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("CELL-A1-TEST-02", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("CELL-A1-TEST-03", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("CELL-A1-TEST-04", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
	}

	@Test
    public void testDaoCellAlterationBulkOn() throws DaoException {
        
        // test with MySQLbulkLoader.isBulkLoad()
		runTheTest();
	}

	@Test
    public void testDaoCellAlterationBulkOff() throws DaoException {
        
        // test without MySQLbulkLoader.isBulkLoad()
        MySQLbulkLoader.bulkLoadOff();
        runTheTest();
        MySQLbulkLoader.bulkLoadOn();
    }
    
    private void runTheTest() throws DaoException{

        //  Add the Sample List
        int numRows = DaoCellProfileSamples.addCellProfileSamples(cellProfileId, internalSampleIds);
        assertEquals(1, numRows);

        //  Add Some Data to EOSINOPHIL cell (uniqueCellId=6)
        //  this Cell must not pre-exists in linear_CRA_test data
        //  otherwise it will cause a duplicated primary key error
        String data = "200:400:600:800";
        String values[] = data.split(":");
        DaoCellAlteration dao = DaoCellAlteration.getInstance();
        numRows = dao.addCellAlterations(cellProfileId, 6, values);
        System.err.println("After SQL execution");
        assertEquals(1, numRows);

        // if bulkLoading, execute LOAD FILE
        if( MySQLbulkLoader.isBulkLoad()){
           MySQLbulkLoader.flushAll();
        }

        HashMap<Integer, String> valueMap = dao.getCellAlterationMap(cellProfileId, 6);
        assertEquals ("200", valueMap.get(internalSampleIds.get(0)));
        assertEquals ("400", valueMap.get(internalSampleIds.get(1)));
        assertEquals ("600", valueMap.get(internalSampleIds.get(2)));
        assertEquals ("800", valueMap.get(internalSampleIds.get(3)));

        //  Test the getCellsInProfile method
        Set <CanonicalCell> cellSet = dao.getCellsInProfile(cellProfileId);
        ArrayList <CanonicalCell> cellList = new ArrayList <CanonicalCell> (cellSet);
        assertEquals (1, cellList.size());
        CanonicalCell cell = cellList.get(0);
        assertEquals ("EOSINOPHIL", cell.getUniqueCellNameAllCaps());
        assertEquals (6, cell.getUniqueCellId());
        
    }
}
