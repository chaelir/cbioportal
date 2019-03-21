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

import org.mskcc.cbio.portal.model.CanonicalCell;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data Access Object to Cell Table.
 * For faster access, consider using DaoCellOptimized.
 *
 * @author Ethan Cerami.
 */
final class DaoCell {

    /**
     * Private Constructor to enforce Singleton Pattern.
     */
    private DaoCell() {
    }
    
    private static int fakeUniqueCellId = 0;
    private static synchronized int getNextFakeUniqueCellId() throws DaoException {
        while (getCell(--fakeUniqueCellId)!=null);
        return fakeUniqueCellId;
    }
    
    public static synchronized int addCellWithoutUniqueCellId(CanonicalCell cell) throws DaoException {
        CanonicalCell existingCell = getCell(cell.getUniqueCellNameAllCaps());
        cell.setUniqueCellId(existingCell==null?getNextFakeUniqueCellId():existingCell.getUniqueCellId());
        return addOrUpdateCell(cell);
    }

    /**
     * Update Cell Record in the Database. 
     * ret: number of rows added, if 0 update a known cell, if 1 add a new cell
     */
    public static int updateCell(CanonicalCell cell) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        boolean setBulkLoadAtEnd = false;
        try {
            //this method only works well with bulk load off, especially 
            //when it is called in a process that may update a cell more than once
            //(e.g. the ImportCellData updates some of the fields based on one 
            // input file and other fields based on another input file):
            setBulkLoadAtEnd = MySQLbulkLoader.isBulkLoad();
            MySQLbulkLoader.bulkLoadOff();
            
            int rows = 0;
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                ("UPDATE IM_cell SET `UNIQUE_CELL_NAME`=?, `TYPE`=?,`ORGAN`=?, `CPID`=?, `ANATOMY_ID`=?, `CELL_TYPE_ID`=? WHERE `UNIQUE_CELL_ID`=?");
// REPLACED                    ("UPDATE cell SET `UNIQUE_CELL_NAME`=?, `TYPE`=?,`ORGAN`=?,`CELL_TYPE_ID`=? WHERE `UNIQUE_CELL_ID`=?");
            pstmt.setString(1, cell.getUniqueCellNameAllCaps());
            pstmt.setString(2, cell.getType());
            pstmt.setString(3, cell.getOrgan());
            pstmt.setInt(4, cell.getCpId());
            pstmt.setString(5, cell.getAnatomyId());
            pstmt.setInt(6, cell.getCellTypeId());
            pstmt.setLong(7, cell.getUniqueCellId());
            rows += pstmt.executeUpdate();
            if (rows != 1) {
                ProgressMonitor.logWarning("No change for " + cell.getUniqueCellId() + " " + cell.getUniqueCellNameAllCaps() + "? Code " + rows);
            }

            return rows;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            if (setBulkLoadAtEnd) {
                //reset to original state:
                MySQLbulkLoader.bulkLoadOn();
            }   
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
        
}
    
    /**
     * Adds a new Cell Record to the Database OR updates the given cell object
     * with the cellEntityId found in the DB for this cell.
     * If it is a new cell, it will generate a new cell entity id and
     * update the given CanonicalCell with the generated 
     * cellEntityId. 
     * 
     * Adds a new Cell Record to the Database.
     *
     * @param cell Canonical Cell Object.
     * @return number of records successfully added.
     * @throws DaoException Database Error.
     */
    public static int addOrUpdateCell(CanonicalCell cell) throws DaoException {

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            int rows = 0;
            CanonicalCell existingCell = getCell(cell.getUniqueCellId());
            if (existingCell == null) {
            	//new cell, so add cell entity first:
            	int cellEntityId = DaoCellEntity.addNewCellEntity(DaoCellEntity.EntityTypes.CELL);
            	//update the Canonical cell as well:
            	cell.setCellEntityId(cellEntityId); //TODO can we find a better way for this, to avoid this side effect? 
            	//add cell, referring to this cell entity
            	con = JdbcUtil.getDbConnection(DaoCell.class);
            	pstmt = con.prepareStatement
                        ("INSERT INTO cell (`CELL_ENTITY_ID`, `UNIQUE_CELL_ID`,`UNIQUE_CELL_NAME`,`TYPE`,`ORGAN`,`CELL_TYPE_ID`) "
                                + "VALUES (?,?,?,?,?,?)");
            	pstmt.setInt(1, cellEntityId);
                pstmt.setLong(2, cell.getUniqueCellId());
                pstmt.setString(3, cell.getUniqueCellNameAllCaps());
                pstmt.setString(4, cell.getType());
                pstmt.setString(5, cell.getOrgan());
                pstmt.setString(6, cell.getAnatomyId());
                rows += pstmt.executeUpdate();

            } else {
            	if (cell.getCellEntityId() == -1) {
	            	//update the Canonical cell  //TODO can we find a better way for this, to avoid this side effect?
	            	cell.setCellEntityId(existingCell.getCellEntityId());
            	} else {
            		//check correctness...normally this error would not occur unless there is an invalid use of CanonicalCell
            		if (cell.getCellEntityId() != existingCell.getCellEntityId())
            			throw new RuntimeException("Unexpected error. Invalid cell entity id for gene: " + cell.getUniqueCellNameAllCaps() + " (" + cell.getCellEntityId() + ")"); 
            	} 
            }	

            rows += addCellAliases(cell);

            return rows;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    /**
     * Add cell_alias records.
     * @param cell Canonical Cell Object.
     * @return number of records successfully added.
     * @throws DaoException Database Error.
     */
    public static int addCellAliases(CanonicalCell cell)  throws DaoException {
        if (MySQLbulkLoader.isBulkLoad()) {
            //  write to the temp file maintained by the MySQLbulkLoader
            Set<String> aliases = cell.getAliases();
            for (String alias : aliases) {
                MySQLbulkLoader.getMySQLbulkLoader("cell_alias").insertRecord(
                        Long.toString(cell.getUniqueCellId()),
                        alias);

            }
            // return 1 because normal insert will return 1 if no error occurs
            return 1;
        }
        
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            Set<String> aliases = cell.getAliases();
            Set<String> existingAliases = getAliases(cell.getUniqueCellId());
            int rows = 0;
            for (String alias : aliases) {
                if (!existingAliases.contains(alias)) {
                    pstmt = con.prepareStatement("INSERT INTO cell_alias "
                            + "(`UNIQUE_CELL_ID`,.CELL_ALIAS`) VALUES (?,?)");
                    pstmt.setLong(1, cell.getUniqueCellId());
                    pstmt.setString(2, alias);
                    rows += pstmt.executeUpdate();
                }
            }

            return rows;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }

    /**
     * Gets the Cell with the Specified Entrez Cell ID.
     * For faster access, consider using DaoCellOptimized.
     *
     * @param uniqueCellId Entrez Cell ID.
     * @return Canonical Cell Object.
     * @throws DaoException Database Error.
     */
    private static CanonicalCell getCell(long uniqueCellId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM cell WHERE UNIQUE_CELL_ID = ?");
            pstmt.setLong(1, uniqueCellId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return extractCell(rs);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    /**
     * Gets aliases for all genes.
     * @return map from entrez cell id to a set of aliases.
     * @throws DaoException Database Error.
     */
    private static Set<String> getAliases(long uniqueCellId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM cell_alias WHERE UNIQUE_CELL_ID = ?");
            pstmt.setLong(1, uniqueCellId);
            rs = pstmt.executeQuery();
            Set<String> aliases = new HashSet<String>();
            while (rs.next()) {
                aliases.add(rs.getString("CELL_ALIAS"));
            }
            return aliases;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    private static Map<Long,Set<String>> getAllAliases() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM cell_alias");
            rs = pstmt.executeQuery();
            Map<Long,Set<String>> map = new HashMap<Long,Set<String>>();
            while (rs.next()) {
                Long entrez = rs.getLong("UNIQUE_CELL_ID");
                Set<String> aliases = map.get(entrez);
                if (aliases==null) {
                    aliases = new HashSet<String>();
                    map.put(entrez, aliases);
                }
                aliases.add(rs.getString("CELL_ALIAS"));
            }
            return map;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    /**
     * Gets all Genes in the Database.
     *
     * @return ArrayList of Canonical Genes.
     * @throws DaoException Database Error.
     */
    public static ArrayList<CanonicalCell> getAllCells() throws DaoException {
        Map<Long,Set<String>> mapAliases = getAllAliases();
        ArrayList<CanonicalCell> geneList = new ArrayList<CanonicalCell>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM gene");
            rs = pstmt.executeQuery();
            while (rs.next()) {
            	int cellEntityId = rs.getInt("CELL_ENTITY_ID");
                long uniqueCellId = rs.getInt("UNIQUE_CELL_ID");
                Set<String> aliases = mapAliases.get(uniqueCellId);
                CanonicalCell cell = new CanonicalCell(cellEntityId, uniqueCellId,
                        rs.getString("UNIQUE_CELL_NAME"), aliases);
                cell.setOrgan(rs.getString("ORGAN"));
                cell.setCellTypeId(rs.getInt("CELL_TYPE_ID"));
                cell.setType(rs.getString("TYPE"));
                geneList.add(cell);
            }
            return geneList;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }

    /**
     * Gets the Cell with the Specified HUGO Cell Symbol.
     * For faster access, consider using DaoCellOptimized.
     *
     * @param hugoGeneSymbol HUGO Cell Symbol.
     * @return Canonical Cell Object.
     * @throws DaoException Database Error.
     */
    private static CanonicalCell getCell(String hugoGeneSymbol) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM cell WHERE UNIQUE_CELL_NAME = ?");
            pstmt.setString(1, hugoGeneSymbol);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return extractCell(rs);
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    private static CanonicalCell extractCell(ResultSet rs) throws SQLException, DaoException {
    	int cellEntityId = rs.getInt("CELL_ENTITY_ID");
    	long uniqueCellId = rs.getInt("UNIQUE_CELL_ID");
            Set<String> aliases = getAliases(uniqueCellId);
            CanonicalCell cell = new CanonicalCell(cellEntityId, uniqueCellId,
                    rs.getString("UNIQUE_CELL_NAME"), aliases);
            cell.setType(rs.getString("TYPE"));
            cell.setOrgan(rs.getString("ORGAN"));
            cell.setCellTypeId(rs.getInt("CELL_TYPE_ID"));
            cell.setAnatomyId(rs.getString("ANATOMY_ID"));
            cell.setCpId(rs.getInt("CPID"));
            
            return cell;
    }

    /**
     * Gets the Number of Cell Records in the Database.
     *
     * @return number of cell records.
     * @throws DaoException Database Error.
     */
    public static int getCount() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement
                    ("SELECT COUNT(*) FROM gene");
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    /**
     * Deletes the Cell Record that has the Entrez Cell ID in the Database.
     * 
     * @param uniqueCellId 
     */
    public static void deleteCell(long uniqueCellId) throws DaoException {
        deleteCellAlias(uniqueCellId);
        
    	Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement("DELETE FROM cell WHERE UNIQUE_CELL_ID=?");
            pstmt.setLong(1, uniqueCellId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
    /**
     * Deletes the Cell Alias Record(s) that has/have the Entrez Cell ID in the Database.
     * 
     * @param uniqueCellId 
     */
    public static void deleteCellAlias(long uniqueCellId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement("DELETE FROM cell_alias WHERE UNIQUE_CELL_ID=?");
            pstmt.setLong(1, uniqueCellId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }

    /**
     * Deletes all Cell Records in the Database.
     *
     * @throws DaoException Database Error.
     * 
     * @deprecated only used by deprecated code, so deprecating this as well.
     */
    public static void deleteAllRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            JdbcUtil.disableForeignKeyCheck(con);
            pstmt = con.prepareStatement("TRUNCATE TABLE gene");
            pstmt.executeUpdate();
            JdbcUtil.enableForeignKeyCheck(con);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
        deleteAllAliasRecords();
    }
    
    /**
     * 
     * @throws DaoException
     * 
     * @deprecated only used by deprecated code, so deprecating this as well.
     */
    private static void deleteAllAliasRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCell.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE cell_alias");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
    
}