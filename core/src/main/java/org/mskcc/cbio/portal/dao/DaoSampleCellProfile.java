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

import org.mskcc.cbio.portal.model.*;

import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.*;

/**
 * Data access object for IM_sample_cell_profile table
 */
public final class DaoSampleCellProfile {
    private DaoSampleCellProfile() {}

    private static final int NO_SUCH_PROFILE_ID = -1;
    private static final String TABLE_NAME = "IM_sample_cell_profile";

    public static int addSampleCellProfile(Integer sampleId, Integer cellProfileId, Integer panelId) throws DaoException {        
        if (MySQLbulkLoader.isBulkLoad()) {
            if (panelId != null) {
                MySQLbulkLoader.getMySQLbulkLoader(TABLE_NAME).insertRecord(Integer.toString(sampleId),
                    Integer.toString(cellProfileId),
                    Integer.toString(panelId));            
            }
            else {
                MySQLbulkLoader.getMySQLbulkLoader(TABLE_NAME).insertRecord(Integer.toString(sampleId),
                    Integer.toString(cellProfileId), null);     
            }

            return 1;
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            if (!sampleExistsInCellProfile(sampleId, cellProfileId)) {
                con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
                pstmt = con.prepareStatement
                        ("INSERT INTO IM_sample_cell_profile (`SAMPLE_ID`, `CELL_PROFILE_ID`, `PANEL_ID`) "
                                + "VALUES (?,?,?)");
                pstmt.setInt(1, sampleId);
                pstmt.setInt(2, cellProfileId);
                if (panelId != null) {
                    pstmt.setInt(3, panelId);
                }
                else {
                    pstmt.setNull(3, java.sql.Types.INTEGER);
                }
                return pstmt.executeUpdate();
            } else {
                return 0;
            }
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    public static boolean sampleExistsInCellProfile(int sampleId, int cellProfileId)
            throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_sample_cell_profile WHERE SAMPLE_ID = ? AND CELL_PROFILE_ID = ?");
            pstmt.setInt(1, sampleId);
            pstmt.setInt(2, cellProfileId);
            rs = pstmt.executeQuery();
            return (rs.next());
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    public static int countSamplesInProfile(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT count(*) FROM IM_sample_cell_profile WHERE CELL_PROFILE_ID = ?");
            pstmt.setInt(1, cellProfileId);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    public static int getCellProfileIdForSample(int sampleId) throws DaoException {
        // TODO: this seems not correct 
        //   a sample could have multiple cell profiles however this only returns the first one
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            pstmt = con.prepareStatement("SELECT CELL_PROFILE_ID FROM IM_sample_cell_profile WHERE SAMPLE_ID = ?");
            pstmt.setInt(1, sampleId);
            rs = pstmt.executeQuery();
            if( rs.next() ) {
               return rs.getInt("CELL_PROFILE_ID");
            }else{
               return NO_SUCH_PROFILE_ID;
            }
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    public static ArrayList<Integer> getAllSampleIdsInProfile(int cellProfileId) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_sample_cell_profile WHERE CELL_PROFILE_ID = ?");
            pstmt.setInt(1, cellProfileId);
            rs = pstmt.executeQuery();
            ArrayList<Integer> sampleIds = new ArrayList<Integer>();
            while (rs.next()) {
                Sample sample = DaoSample.getSampleById(rs.getInt("SAMPLE_ID"));
                sampleIds.add(rs.getInt("SAMPLE_ID"));
            }
            return sampleIds;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    public static ArrayList<Integer> getAllSamples() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_sample_cell_profile");
            rs = pstmt.executeQuery();
            ArrayList<Integer> sampleIds = new ArrayList<Integer>();
            while (rs.next()) {
                sampleIds.add(rs.getInt("SAMPLE_ID"));
            }
            return sampleIds;
        } catch (NullPointerException e) {
            throw new DaoException(e);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    /**
     * Counts the number of sequenced cases in each cancer study, returning a list of maps
     * containing the cancer study (full string name), the cancer type (e.g. `brca_tcga`), and the count
     *
     * @return  [ list of maps { cancer_study, cancer_type, num_sequenced_samples } ]
     * @throws DaoException
     * @author Gideon Dresdner <dresdnerg@cbio.mskcc.org>
     *
     */
    /* disabled mutation stuff
    public static List<Map<String, Object>> metaData(List<CancerStudy> cancerStudies) throws DaoException {
        // collect all mutationProfileIds
        Map<Integer, CellProfile> id2MutationProfile = new HashMap<Integer, CellProfile>();
        for (CancerStudy cancerStudy : cancerStudies) {
            CellProfile mutationProfile = cancerStudy.getMutationProfile();

            if (mutationProfile != null) {
                // e.g. if cancerStudy == All Cancer Studies
                Integer mutationProfileId = mutationProfile.getCellProfileId();
                id2MutationProfile.put(mutationProfileId, mutationProfile);
            }
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
        try {
            con = JdbcUtil.getDbConnection(DaoMutation.class);

            String sql = "select `CELL_PROFILE_ID`, count(`SAMPLE_ID`) from IM_sample_cell_profile " +
                    " where `CELL_PROFILE_ID` in ("+ StringUtils.join(id2MutationProfile.keySet(), ",") + ")" +
                    " group by `CELL_PROFILE_ID`";

            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                Map<String, Object> datum = new HashMap<String, Object>();

                Integer mutationProfileId = rs.getInt(1);
                Integer numSequencedSamples = rs.getInt(2);

                CellProfile mutationProfile = id2MutationProfile.get(mutationProfileId);
                Integer cancerStudyId = mutationProfile.getCancerStudyId();
                CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByInternalId(cancerStudyId);
                String cancerStudyName = cancerStudy.getName();
                String cancerType = cancerStudy.getTypeOfCancerId();

                datum.put("cancer_study", cancerStudyName);
                datum.put("cancer_type", cancerType);
                datum.put("color", DaoTypeOfCancer.getTypeOfCancerById(cancerType).getDedicatedColor());
                datum.put("num_sequenced_samples", numSequencedSamples);

                data.add(datum);
            }

        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoMutation.class, con, pstmt, rs);
        }

    return data;
    }
    */

    public static void deleteAllRecords() throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            pstmt = con.prepareStatement("TRUNCATE TABLE IM_sample_cell_profile");
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }

    public static void deleteRecords(List<Integer> sampleIds, List<Integer> profileIds) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoSampleCellProfile.class);
            for (int i = 0; i < sampleIds.size(); i++) {
                pstmt = con.prepareCall("DELETE FROM IM_sample_cell_profile WHERE sample_id = ? and cell_profile_id = ?");
                pstmt.setInt(1, sampleIds.get(i));
                pstmt.setInt(2, profileIds.get(i));
                pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoSampleCellProfile.class, con, pstmt, rs);
        }
    }
}
