package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DaoCellEntity {

	/**
     * Private Constructor to enforce Singleton Pattern.
     */
    private DaoCellEntity() {
    }
    
    public static enum EntityTypes
    {
        CELL;
    }
    
    /**
     * Adds a new cell entity Record to the Database and 
     * returns the auto generated id value.
     *
     * @param entityType : one of EntityTypes
     * @return : auto generated cell entity id value
     * @throws DaoException Database Error.
     */
    public static int addNewCellEntity(EntityTypes entityType) throws DaoException {

        System.err.println("START addNewCellEntity");
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            System.err.println("addNewCellEntity: getDbConnection");
            con = JdbcUtil.getDbConnection(DaoCell.class);
            //NOTE: this sql requires auto_increment of ID column in the table schema 
            System.err.println("addNewCellEntity: prepareStatement");
        	pstmt = con.prepareStatement
                ("INSERT INTO IM_cell_entity (`ENTITY_TYPE`) "
                    + "VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        	pstmt.setString(1, entityType.name());
            System.err.println("addNewCellEntity: executeUpdate");
            pstmt.executeUpdate();
            //get the auto generated key:
            rs = pstmt.getGeneratedKeys();
            rs.next();
            int newId = rs.getInt(1);
            return newId;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCell.class, con, pstmt, rs);
        }
    }
}
