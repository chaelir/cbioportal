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
	
	@Before
	public void setUp() throws DaoException {
		study = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
		cellProfileId = DaoCellProfile.getCellProfileByStableId("linear_CRA").getCellProfileId();
		
		internalSampleIds = new ArrayList<Integer>();
        Patient p = new Patient(study, "TCGA-1");
        int pId = DaoPatient.addPatient(p);
        
        DaoSample.reCache();
        Sample s = new Sample("XCGA-A1-A0SB-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("XCGA-A1-A0SD-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("XCGA-A1-A0SE-01", pId, "brca");
        internalSampleIds.add(DaoSample.addSample(s));
        s = new Sample("XCGA-A1-A0SF-01", pId, "brca");
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

        //  Add Some Data to B-cell (uniqueCellId=1)
        String data = "200:400:600:800";
        String values[] = data.split(":");
        DaoCellAlteration dao = DaoCellAlteration.getInstance();
        numRows = dao.addCellAlterations(cellProfileId, 1, values); 
        assertEquals(1, numRows);

        // if bulkLoading, execute LOAD FILE
        if( MySQLbulkLoader.isBulkLoad()){
           MySQLbulkLoader.flushAll();
        }

        HashMap<Integer, String> valueMap = dao.getCellAlterationMap(cellProfileId, 1);
        assertEquals ("200", valueMap.get(internalSampleIds.get(0)));
        assertEquals ("400", valueMap.get(internalSampleIds.get(1)));
        assertEquals ("600", valueMap.get(internalSampleIds.get(2)));
        assertEquals ("800", valueMap.get(internalSampleIds.get(3)));

        //  Test the getCellsInProfile method
        Set <CanonicalCell> cellSet = dao.getCellsInProfile(cellProfileId);
        ArrayList <CanonicalCell> cellList = new ArrayList <CanonicalCell> (cellSet);
        assertEquals (1, cellList.size());
        CanonicalCell cell = cellList.get(0);
        assertEquals ("B CELL CPID=1", cell.getUniqueCellNameAllCaps());
        assertEquals (1, cell.getUniqueCellId());
        
    }
}
