/* LICENSE_TBD */

package org.mskcc.cbio.portal.dao;

import java.sql.*;
import java.util.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.SpringUtil;

/**
 * Analogous to and replaces the old DaoCancerType. A CancerStudy has a NAME and
 * DESCRIPTION. If PUBLIC is true a CancerStudy can be accessed by anyone,

/**
 * Data access object for IM_Cell_Profile table
 */
public final class DaoCellProfile {
    private DaoCellProfile() {}
    
    private static final Map<String,CellProfile> byStableId = new HashMap<String,CellProfile>();
    private static final Map<Integer,CellProfile> byInternalId = new HashMap<Integer,CellProfile>();
    private static final Map<Integer,List<CellProfile>> byStudy = new HashMap<Integer,List<CellProfile>>();

    static {
        SpringUtil.initDataSource();
        reCache();
    }

    public static synchronized void reCache() {
        byStableId.clear();
        byInternalId.clear();
        byStudy.clear();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfile.class);

            pstmt = con.prepareStatement
                    ("SELECT * FROM IM_cell_profile");
            rs = pstmt.executeQuery();
            while (rs.next()) {
                CellProfile profileType = extractCellProfile(rs);
                cacheCellProfile(profileType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.closeAll(DaoCellProfile.class, con, pstmt, rs);
        }
    }
    
    private static void cacheCellProfile(CellProfile profile) {
        byStableId.put(profile.getStableId(), profile);
        byInternalId.put(profile.getCellProfileId(), profile);
        List<CellProfile> list = byStudy.get(profile.getCancerStudyId());
        if (list==null) {
            list = new ArrayList<CellProfile>();
            byStudy.put(profile.getCancerStudyId(), list);
        }
        list.add(profile);
    }
   
    public static int addCellProfile(CellProfile profile) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int rows = 0;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfile.class);

            pstmt = con.prepareStatement
                    ("INSERT INTO IM_cell_profile (`STABLE_ID`, `CANCER_STUDY_ID`, `CELL_ALTERATION_TYPE`," +
                            "`DATATYPE`, `NAME`, `DESCRIPTION`, `SHOW_PROFILE_IN_ANALYSIS_TAB`) " +
                            "VALUES (?,?,?,?,?,?,?)");
            pstmt.setString(1, profile.getStableId());
            pstmt.setInt(2, profile.getCancerStudyId());
            pstmt.setString(3, profile.getCellAlterationType().name());
            pstmt.setString(4, profile.getDatatype());
            pstmt.setString(5, profile.getProfileName());
            pstmt.setString(6, profile.getProfileDescription());
            pstmt.setBoolean(7, profile.showProfileInAnalysisTab());
            rows = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfile.class, con, pstmt, rs);
        }
        
        reCache();
        return rows;
    }

    /**
     * Updates a Cell Profile Name and Description.
     * @param cellProfileId     Cell Profile ID.
     * @param name              New Cell Profile Name.
     * @param description       New Cell Profile Description.
     * @return                  Returns True if Cell Profile was Updated.
     * @throws DaoException     Data Access Error.
     */
    public static boolean updateNameAndDescription (int cellProfileId, String name, String description)
        throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        boolean ret = false;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfile.class);
            pstmt = con.prepareStatement("UPDATE IM_cell_profile SET NAME=?, DESCRIPTION=? " +
                    "WHERE CELL_PROFILE_ID=?");
            pstmt.setString(1, name);
            pstmt.setString(2, description);
            pstmt.setInt(3, cellProfileId);
            ret = pstmt.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfile.class, con, pstmt, rs);
        }
        
        reCache();
        return ret;
    }
    
    public static int deleteCellProfile(CellProfile profile) throws DaoException {
       int rows = 0;
       Connection con = null;
       PreparedStatement pstmt = null;
       ResultSet rs = null;
       try {
           con = JdbcUtil.getDbConnection(DaoCellProfile.class);
           pstmt = con.prepareStatement("DELETE FROM IM_cell_profile WHERE STABLE_ID = ?");
           // NOTE: assumed this will cascade to IM_cell_profile_samples and IM_sample_cell_profile.
           pstmt.setString(1, profile.getStableId());
           rows = pstmt.executeUpdate();
       } catch (SQLException e) {
           throw new DaoException(e);
       } finally {
           JdbcUtil.closeAll(DaoCellProfile.class, con, pstmt, rs);
       }
       
       reCache();
       return rows;
   }
    
    public static CellProfile getCellProfileByStableId(String stableId) {
        return byStableId.get(stableId);
    }

    public static CellProfile getCellProfileById(int cellProfileId) {
        return byInternalId.get(cellProfileId);
    }
    
    // TODO: UNIT TEST
    public static ArrayList <CellProfile> getCellProfiles (int[] cellProfileIds) throws
      DaoException {
        ArrayList <CellProfile> cellProfileList = new ArrayList <CellProfile>();
        for (int cellProfileId:  cellProfileIds) {
            CellProfile cellProfile =
                    DaoCellProfile.getCellProfileById(cellProfileId);
            if (cellProfile != null) {
                cellProfileList.add(cellProfile);
            } else {
                throw new IllegalArgumentException ("Could not find cell profile for:  "
                        + cellProfileId);
            }
        }
        return cellProfileList;
    }

    public static int getCount() {
        return byStableId.size();
    }

    private static CellProfile extractCellProfile(ResultSet rs) throws SQLException {
        CellProfile profileType = new CellProfile();
        profileType.setStableId(rs.getString("STABLE_ID"));

        profileType.setCancerStudyId(rs.getInt("CANCER_STUDY_ID"));
        profileType.setProfileName(rs.getString("NAME"));
        profileType.setProfileDescription(rs.getString("DESCRIPTION"));
        try {
            profileType.setShowProfileInAnalysisTab(rs.getBoolean("SHOW_PROFILE_IN_ANALYSIS_TAB"));
        } catch (SQLException e) {
            profileType.setShowProfileInAnalysisTab(true);
        }
        profileType.setCellAlterationType(CellAlterationType.valueOf(rs.getString("CELL_ALTERATION_TYPE")));
        profileType.setDatatype(rs.getString("DATATYPE"));
        profileType.setCellProfileId(rs.getInt("CELL_PROFILE_ID"));
        return profileType;
    }

    public static ArrayList<CellProfile> getAllCellProfiles(int cancerStudyId) {
        List<CellProfile> list = byStudy.get(cancerStudyId);
        if (list==null) {
            return new ArrayList<CellProfile>();
        }
        
        // TODO: refactor the code to use List
        return new ArrayList<CellProfile>(list);
    }

    public static void deleteAllRecords() throws DaoException {
        byStableId.clear();
        byInternalId.clear();
        byStudy.clear();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = JdbcUtil.getDbConnection(DaoCellProfile.class);
            JdbcUtil.disableForeignKeyCheck(con);
            pstmt = con.prepareStatement("TRUNCATE TABLE IM_cell_profile");
            pstmt.executeUpdate();
            JdbcUtil.enableForeignKeyCheck(con);
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfile.class, con, pstmt, rs);
        }
    }
}
