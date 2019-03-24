/* LICENSE_TBD */

package org.mskcc.cbio.portal.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Data Access Objects for the Cell Profile Samples Table.
 *
 * @author Ethan Cerami.
 */
public final class DaoCellProfileSamples
{
    private static final String DELIM = ",";

    private DaoCellProfileSamples() {}

    /**
     * Adds a new Ordered Sample List for a Specified Cell Profile ID.
     *
     * @param cellProfileId  Cell Profile ID.
     * @param orderedSampleList   Array List of Sample IDs.
     * @return number of rows added.
     * @throws DaoException Data Access Exception.
     */
    public static int addCellProfileSamples(int cellProfileId, ArrayList<Integer> orderedSampleList)
            throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuffer orderedSampleListBuf = new StringBuffer();
        //  Created Joined String, based on DELIM token
        for (Integer sampleId :  orderedSampleList) {
            orderedSampleListBuf.append(Integer.toString(sampleId)).append(DELIM);
        }
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfileSamples.class);
            pstmt = con.prepareStatement
                    ("INSERT INTO IM_cell_profile_samples (`CELL_PROFILE_ID`, " +
                    "`ORDERED_SAMPLE_LIST`) "+ "VALUES (?,?)");
            pstmt.setInt(1, cellProfileId);
            pstmt.setString(2, orderedSampleListBuf.toString());
            return pstmt.executeUpdate();
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfileSamples.class, con, pstmt, rs);
        }
    }

    /**
     * Deletes all samples associated with the specified Cell Profile ID.
     *
     * @param cellProfileId Cell Profile ID.
     * @throws DaoException Database Error.
     */
    public static void deleteAllSamplesInCellProfile(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfileSamples.class);
            pstmt = con.prepareStatement("DELETE from " +
                    "IM_cell_profile_samples WHERE CELL_PROFILE_ID=?");
            pstmt.setLong(1, cellProfileId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfileSamples.class, con, pstmt, rs);
        }
    }

    /**
     * Gets an Ordered Sample List for the specified Cell Profile ID.
     *
     * @param cellProfileId Cell Profile ID.
     * @throws DaoException Database Error.
     */
    public static ArrayList <Integer> getOrderedSampleList(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfileSamples.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_cell_profile_samples WHERE CELL_PROFILE_ID = ?");
            pstmt.setInt(1, cellProfileId);
            rs = pstmt.executeQuery();
            if  (rs.next()) {
                String orderedSampleList = rs.getString("ORDERED_SAMPLE_LIST");

                //  Split, based on DELIM token
                String parts[] = orderedSampleList.split(DELIM);
                ArrayList <Integer> sampleList = new ArrayList <Integer>();
                for (String internalSampleId : parts) {
                    sampleList.add(Integer.parseInt(internalSampleId));
                }
                return sampleList;
            } else {
                return new ArrayList<Integer>();
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfileSamples.class, con, pstmt, rs);
        }
    }

    /**
     * Deletes all Records in the table.
     * @throws DaoException Database Exception.
     */
    public static void deleteAllRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfileSamples.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE IM_cell_profile_samples");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfileSamples.class, con, pstmt, rs);
        }
    }
}
