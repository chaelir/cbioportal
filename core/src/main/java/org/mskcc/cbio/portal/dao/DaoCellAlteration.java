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

import org.codehaus.jackson.node.ObjectNode;
import org.mskcc.cbio.portal.model.CanonicalCell;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

/**
 * Data Access Object for the Cell Alteration Table.
 *
 * @author Ethan Cerami.
 */
public class DaoCellAlteration {
    private static final String DELIM = ",";
    public static final String NAN = "NaN";
    private static DaoCellAlteration daoCellAlteration = null;

    /**
     * Private Constructor (Singleton pattern).
     */
    private DaoCellAlteration() {
    }

    /**
     * Gets Instance of Dao Object. (Singleton pattern).
     *
     * @return DaoCellAlteration Object.
     * @throws DaoException Dao Initialization Error.
     */
    public static DaoCellAlteration getInstance() throws DaoException {
        if (daoCellAlteration == null) {
            daoCellAlteration = new DaoCellAlteration();
            
        }

        return daoCellAlteration;
    }

    public static interface AlterationProcesser {
        ObjectNode process(
            long uniqueCellId,
            String[] values,
            ArrayList<Integer> orderedSampleList
        );
    }

    /**
     * Adds a Row of Cell Alterations associated with a Cell Profile ID and Unique Cell ID.
     * @param cellProfileId Cell Profile ID.
     * @param uniqueCellId Unique Cell ID.
     * @param values DELIM separated values.
     * @return number of rows successfully added.
     * @throws DaoException Database Error.
     */
    public int addCellAlterations(int cellProfileId, long uniqueCellId, String[] values)
            throws DaoException {
    	return addCellAlterationsForCellEntity(cellProfileId, DaoCellOptimized.getCellEntityId(uniqueCellId), values);
    }
    
    public int addCellAlterationsForCellEntity(int cellProfileId, int cellEntityId, String[] values)
            throws DaoException {
    
        StringBuffer valueBuffer = new StringBuffer();
        for (String value:  values) {
            if (value.contains(DELIM)) {
                throw new IllegalArgumentException ("Value cannot contain delim:  " + DELIM
                    + " --> " + value);
            }
            valueBuffer.append(value).append(DELIM);
        }
        
       if (MySQLbulkLoader.isBulkLoad() ) {
          //  write to the temp file maintained by the MySQLbulkLoader
          MySQLbulkLoader.getMySQLbulkLoader("IM_cell_alteration").insertRecord(Integer.toString( cellProfileId ),
        		  Integer.toString( cellEntityId ), valueBuffer.toString());
          // return 1 because normal insert will return 1 if no error occurs
          return 1;
        } 
       
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement
                    ("INSERT INTO IM_cell_alteration (CELL_PROFILE_ID, " +
                            " CELL_ENTITY_ID," +
                            " `VALUES`) "
                            + "VALUES (?,?,?)");
            pstmt.setInt(1, cellProfileId);
            pstmt.setLong(2, cellEntityId);
            pstmt.setString(3, valueBuffer.toString());
            System.err.println("Before SQL execution");
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Gets the Specified Cell Alteration.
     *
     * @param cellProfileId  Cell Profile ID.
     * @param sampleId            Sample ID.
     * @param uniqueCellId      Unique Cell ID.
     * @return value or NAN.
     * @throws DaoException Database Error.
     */
    public String getCellAlteration(int cellProfileId, int sampleId,
            long uniqueCellId) throws DaoException {
        HashMap <Integer, String> sampleMap = getCellAlterationMap(cellProfileId, uniqueCellId);
        if (sampleMap.containsKey(sampleId)) {
            return sampleMap.get(sampleId);
        } else {
            return NAN;
        }
    }

    /**
     * Gets a HashMap of Values, keyed by Sample ID.
     * @param cellProfileId  Cell Profile ID.
     * @param uniqueCellId      Unique Cell ID.
     * @return HashMap of values, keyed by Sample ID.
     * @throws DaoException Database Error.
     */
    public HashMap<Integer, String> getCellAlterationMap(int cellProfileId,
            long uniqueCellId) throws DaoException {
        HashMap<Long,HashMap<Integer, String>> map = getCellAlterationMap(cellProfileId, Collections.singleton(uniqueCellId));
        if (map.isEmpty()) {
            return new HashMap<Integer, String>();
        }
        
        return map.get(uniqueCellId);
    }

    /**
     * Returns the map of uniqueCellId as key and map with all
     * respective CaseId and Values as value. 
     * 
     * @param cellProfileId  Cell Profile ID.
     * @param uniqueCellIds      Unique Cell IDs.
     * @return Map<Unique, Map<CaseId, Value>>.
     * @throws DaoException Database Error.
     */
    public HashMap<Long,HashMap<Integer, String>> getCellAlterationMap(int cellProfileId, Collection<Long> uniqueCellIds) throws DaoException {
    	Collection<Integer> cellEntityIds = null;
    	if (uniqueCellIds != null) {
    		//translate uniqueCellIds to corresponding cellEntityIds:
        	cellEntityIds = new ArrayList<Integer>();
	    	for (Long uniqueCellId : uniqueCellIds) {
	    		cellEntityIds.add(DaoCellOptimized.getCellEntityId(uniqueCellId));
	    	}
    	}
    	HashMap<Integer, HashMap<Integer, String>> intermediateMap = getCellAlterationMapForEntityIds(cellProfileId, cellEntityIds);
    	//translate back to unique, since intermediateMap is keyed by cellEntityIds:
    	HashMap<Long, HashMap<Integer, String>> resultMap = new HashMap<Long, HashMap<Integer, String>>();
    	Iterator<Entry<Integer, HashMap<Integer, String>>> mapIterator = intermediateMap.entrySet().iterator();
    	while (mapIterator.hasNext()) {
    		Entry<Integer, HashMap<Integer, String>> mapEntry = mapIterator.next();
    		resultMap.put(DaoCellOptimized.getUniqueCellId(mapEntry.getKey()), mapEntry.getValue());
    	}    	
    	return resultMap;
    }
    
    /**
     * Returns the map of cellEntityIds as key and map with all
     * respective CaseId and Values as value. 
     * 
     * @param cellProfileId
     * @param cellEntityIds
     * @return Map<CellEntityId, Map<CaseId, Value>>.
     * @throws DaoException
     */
    public HashMap<Integer,HashMap<Integer, String>> getCellAlterationMapForEntityIds(int cellProfileId, Collection<Integer> cellEntityIds) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        HashMap<Integer,HashMap<Integer, String>> map = new HashMap<Integer,HashMap<Integer, String>>();
        ArrayList<Integer> orderedSampleList = DaoCellProfileSamples.getOrderedSampleList(cellProfileId);
        if (orderedSampleList == null || orderedSampleList.size() ==0) {
            throw new IllegalArgumentException ("Could not find any samples for cell" +
                    " profile ID:  " + cellProfileId);
        }
        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            if (cellEntityIds == null) {
                pstmt = con.prepareStatement("SELECT * FROM IM_cell_alteration WHERE"
                        + " CELL_PROFILE_ID = " + cellProfileId);
            } else {
                pstmt = con.prepareStatement("SELECT * FROM IM_cell_alteration WHERE"
                        + " CELL_PROFILE_ID = " + cellProfileId
                        + " AND CELL_ENTITY_ID IN ("+StringUtils.join(cellEntityIds, ",")+")");
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                HashMap<Integer, String> mapSampleValue = new HashMap<Integer, String>();
                int cellEntityId = rs.getInt("CELL_ENTITY_ID");
                String values = rs.getString("VALUES");
                //hm.debug..
                String valueParts[] = values.split(DELIM);
                for (int i=0; i<valueParts.length; i++) {
                    String value = valueParts[i];
                    Integer sampleId = orderedSampleList.get(i);
                    mapSampleValue.put(sampleId, value);
                }
                map.put(cellEntityId, mapSampleValue);
            }
            return map;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Process SQL result alteration data
     * @param cellProfileId  Cell Profile ID.
     * @param processor         Implementation of AlterationProcesser Interface
     * @return ArrayList<ObjectNode>
     * @throws DaoException Database Error, MathException
     */
    public static ArrayList<ObjectNode> getProcessedAlterationData(
            int cellProfileId,               //queried profile internal id (num)
            //Set<Long> uniqueCellIds,            //list of cells in calculation cell pool (all cells or only cancer cells)
            int offSet,                         //OFFSET for LIMIT (to get only one segment of the cells)
            AlterationProcesser processor       //implemented interface
    ) throws DaoException {

        ArrayList<ObjectNode> result = new ArrayList<>();

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        ArrayList<Integer> orderedSampleList = DaoCellProfileSamples.getOrderedSampleList(cellProfileId);
        if (orderedSampleList == null || orderedSampleList.size() ==0) {
            throw new IllegalArgumentException ("Could not find any samples for cell" +
                    " profile ID:  " + cellProfileId);
        }

        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);

            pstmt = con.prepareStatement("SELECT * FROM IM_cell_alteration WHERE"
                    + " CELL_PROFILE_ID = " + cellProfileId
                    + " LIMIT 3000 OFFSET " + offSet);

            rs = pstmt.executeQuery();
            while (rs.next()) {
                long uniqueCellId = DaoCellOptimized.getUniqueCellId(rs.getInt("CELL_ENTITY_ID"));
                String[] values = rs.getString("VALUES").split(DELIM);
                ObjectNode datum = processor.process(
                        uniqueCellId,
                        values,
                        orderedSampleList);
                if (datum != null) result.add(datum);
            }
            return result;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }

    }

    /**
     * Gets all Cells in a Specific Cell Profile.
     * @param cellProfileId  Cell Profile ID.
     * @return Set of Canonical Cells.
     * @throws DaoException Database Error.
     */
    public Set<CanonicalCell> getCellsInProfile(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Set <CanonicalCell> cellList = new HashSet <CanonicalCell>();
        DaoCellOptimized daoCell = DaoCellOptimized.getInstance();

        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_cell_alteration WHERE CELL_PROFILE_ID = ?");
            pstmt.setInt(1, cellProfileId);

            rs = pstmt.executeQuery();
            while  (rs.next()) {
                Long uniqueCellId = DaoCellOptimized.getUniqueCellId(rs.getInt("CELL_ENTITY_ID"));
                cellList.add(daoCell.getCell(uniqueCellId));
            }
            return cellList;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Gets all Cells in a Specific Cell Profile.
     * @param cellProfileId  Cell Profile ID.
     * @return Set of Canonical Cells.
     * @throws DaoException Database Error.
     */
    public static Set<Integer> getEntityIdsInProfile(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Set<Integer> cellEntityList = new HashSet<>();

        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_cell_alteration WHERE CELL_PROFILE_ID = ?");
            pstmt.setInt(1, cellProfileId);

            rs = pstmt.executeQuery();
            while  (rs.next()) {
            	int cellEntityId = rs.getInt("CELL_ENTITY_ID");
                cellEntityList.add(cellEntityId);
            }
            return cellEntityList;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Gets the total number of all cells in a Specific Cell Profile.
     * @param cellProfileId  Cell Profile ID.
     * @return number of Canonical Cells.
     * @throws DaoException Database Error.
     */
    public static int getCellsCountInProfile(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement
                    ("SELECT COUNT(*) FROM IM_cell_alteration WHERE CELL_PROFILE_ID = ?");
            pstmt.setInt(1, cellProfileId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Gets total number of records in table.
     * @return number of records.
     * @throws DaoException Database Error.
     */
    public int getCount() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement
                    ("SELECT COUNT(*) FROM IM_cell_alteration");
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Deletes all Cell Alteration Records associated with the specified Cell Profile ID.
     *
     * @param cellProfileId Cell Profile ID.
     * @throws DaoException Database Error.
     */
    public void deleteAllRecordsInCellProfile(long cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement("DELETE from " +
                    "IM_cell_alteration WHERE CELL_PROFILE_ID=?");
            pstmt.setLong(1, cellProfileId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }

    /**
     * Deletes all Records in Table.
     *
     * @throws DaoException Database Error.
     */
    public void deleteAllRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellAlteration.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE IM_cell_alteration");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellAlteration.class, con, pstmt, rs);
        }
    }
}
