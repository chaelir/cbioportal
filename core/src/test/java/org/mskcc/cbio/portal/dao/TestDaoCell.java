/* LICENSE_TBD */

package org.mskcc.cbio.portal.dao;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoCellOptimized;
import org.mskcc.cbio.portal.dao.MySQLbulkLoader;
import org.mskcc.cbio.portal.model.CanonicalCell;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.Assert.*;

import java.util.HashSet;

/**
 * JUnit Tests for DaoCell and DaoCellOptimized.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/applicationContext-dao.xml" })
@TransactionConfiguration(transactionManager = "transactionManager", defaultRollback = true)
@Transactional
public class TestDaoCell {

		/**
		 * Tests DaoCell and DaoCellOptimized.
		 * @throws DaoException Database Error.
		 */
	@Test
	public void testAddExistingCell() throws DaoException {

		// save bulkload setting before turning off
		boolean isBulkLoad = MySQLbulkLoader.isBulkLoad();
		MySQLbulkLoader.bulkLoadOff();

		//	Add Existing Cells, uniqueCellId=1
		CanonicalCell cell = new CanonicalCell(1, "a_test_cell",
		  new HashSet<String>(Arrays.asList("a_test_cell_alias1|a_test_cell_alias2|a_test_cell_alias3".split("\\|"))));
		DaoCellOptimized daoCellOptimized = DaoCellOptimized.getInstance();
		int num = daoCellOptimized.addCell(cell); 
		assertEquals(3, num); //NOTE: returns # of added rows, +1 cell and +3 alias

		// restore bulk setting
		if (isBulkLoad) {
			MySQLbulkLoader.bulkLoadOn();
		}
	}

		/**
		 * Tests DaoCell and DaoCellOptimized.
		 * @throws DaoException Database Error.
		 */
	@Test
	public void testAddNewCell() throws DaoException {

		// save bulkload setting before turning off
		boolean isBulkLoad = MySQLbulkLoader.isBulkLoad();
		MySQLbulkLoader.bulkLoadOff();
		
        // Add New Cells, uniqueCellId = 0
        // NOTE: set uniqueCellId <= 0 will effect addCell to addCellWithoutUniqueCellId() 
        //       which will generate a negative fake unique cell id  
		CanonicalCell cell = new CanonicalCell(0, "b_test_cell",
				new HashSet<String>(Arrays.asList("b_test_cell_alias1|b_test_cell_alias2|b_test_cell_alias3".split("\\|"))));
		DaoCellOptimized daoCellOptimized = DaoCellOptimized.getInstance();
		int num = daoCellOptimized.addCell(cell);
		assertEquals(4, num); //NOTE: returns # of added records, +4 for a new cell  

		// restore bulk setting
		if (isBulkLoad) {
			MySQLbulkLoader.bulkLoadOn();
		}
	}

	/**
	 * Validates Monocyte by ID.
	 */
	@Test
	public void testMonocyteById() {
		CanonicalCell cell = DaoCellOptimized.getInstance().getCell(13);
		assertEquals("MONOCYTE", cell.getUniqueCellNameAllCaps());
		assertEquals(13, cell.getUniqueCellId());
	}

	/**
	 * Validates Macrophage by ID
	 */
	@Test
	public void testMacrophageById() {
		CanonicalCell cell = DaoCellOptimized.getInstance().getCell(9);
		assertEquals("MACROPHAGE", cell.getUniqueCellNameAllCaps());
		assertEquals(9, cell.getUniqueCellId());
	}

	/**
	 * Validates Macrophage by Name
	 */
	@Test
	public void testMacrophageByName() {
		CanonicalCell cell = DaoCellOptimized.getInstance().getCell("Macrophage");
		assertEquals("MACROPHAGE", cell.getUniqueCellNameAllCaps());
		assertEquals(9, cell.getUniqueCellId());
	}

}
