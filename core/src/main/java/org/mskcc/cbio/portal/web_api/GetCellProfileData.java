/*
 * Copyright (c) 2015 - 2016 Memorial Sloan-Kettering Cancer Center.
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

package org.mskcc.cbio.portal.web_api;

import java.io.IOException;
import java.util.*;
import org.json.simple.JSONArray;
import org.mskcc.cbio.io.WebFileConnect;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.servlet.WebService;
import org.mskcc.cbio.portal.util.*;

/**
 * Web Service to Get Profile Data.
 *
 * @author Ethan Cerami.
 */
public class GetCellProfileData {
    public static final int ID_UNIQUE_CELL = 1;
    public static final int CELL_NAME = 0;
    private String rawContent;
    private String[][] matrix;
    private ProfileData profileData;
    private List<String> warningList = new ArrayList<String>();

    /**
     * Constructor.
     * @param targetCellProfileIdList    Target Cell Profile List.
     * @param targetCellList                Target Cell List.
     * @param targetSampleList             Target Sample List.
     * @param suppressMondrianHeader        Flag to suppress the mondrian header.
     * @throws DaoException Database Error.
     * @throws IOException IO Error.
     */
    public GetCellProfileData (List<String> targetCellProfileIdList,
            List<String> targetCellList,
            List<String> targetSampleList,
            Boolean suppressMondrianHeader)
            throws DaoException, IOException {
        execute(targetCellProfileIdList, targetCellList, targetSampleList, suppressMondrianHeader);
    }

    /**
     * Constructor.
     *
     * @param cellProfile    Cell Profile Object.
     * @param targetCellList    Target Cell List.
     * @param sampleIds        White-space delimited sample IDs.
     * @throws DaoException     Database Error.
     * @throws IOException      IO Error.
     */
    public GetCellProfileData (CellProfile cellProfile, List<String> targetCellList,
            String sampleIds) throws DaoException, IOException {
        List<String> targetCellProfileIdList = new ArrayList<String>();
        targetCellProfileIdList.add(cellProfile.getStableId());

        List<String> targetSampleList = new ArrayList<String>();
        String sampleIdParts[] = sampleIds.split("\\s+");
        for (String sampleIdPart : sampleIdParts) {
            targetSampleList.add(sampleIdPart);
        }
        execute(targetCellProfileIdList, targetCellList, targetSampleList, true);
    }

    /**
     * Gets the Raw Content Produced by the Web API.
     * @return Raw Content.
     */
    public String getRawContent() {
        return rawContent;
    }

    /**
     * Gets the Data Matrix Produced by the Web API.
     * @return Matrix of Strings.
     */
    public String[][] getMatrix() {
        return matrix;
    }

    /**
     * Gets the Profile Data Object Produced by the Web API.
     * @return ProfileData Object.
     */
    public ProfileData getProfileData() {
        return profileData;
    }

    /**
     * Gets warnings (if triggered).
     *
     * @return List of Warning Strings.
     */
    public List<String> getWarnings() {
        return this.warningList;
    }

    /**
     * Executes the LookUp.
     */
    private void execute(List<String> targetCellProfileIdList,
            List<String> targetCellList, List<String> targetSampleList,
            Boolean suppressMondrianHeader) throws DaoException, IOException {
        this.rawContent = getProfileData (targetCellProfileIdList, targetCellList,
                targetSampleList, suppressMondrianHeader);
        this.matrix = WebFileConnect.parseMatrix(rawContent);

        //  Create the Profile Data Object
        if (targetCellProfileIdList.size() == 1) {
            String cellProfileId = targetCellProfileIdList.get(0);
            CellProfile cellProfile =
                    DaoCellProfile.getCellProfileByStableId(cellProfileId);
            profileData = new ProfileData(cellProfile, matrix);
        }
    }

    /**
     * Gets Profile Data for Specified Target Info.
     * @param targetCellProfileIdList    Target Cell Profile List.
     * @param targetCellList                Target Cell List.
     * @param targetSampleList             Target Sample List.
     * @param suppressMondrianHeader        Flag to suppress the mondrian header.
     * @return Tab Delim Text String.
     * @throws DaoException Database Error.
     */
    private String getProfileData(List<String> targetCellProfileIdList,
            List<String> targetCellList, 
            List<String> targetSampleList,
            Boolean suppressMondrianHeader) throws DaoException {

        StringBuffer buf = new StringBuffer();

        //  Validate that all Cell Profiles are valid Stable IDs.
        for (String cellProfileId:  targetCellProfileIdList) {
            CellProfile cellProfile =
                    DaoCellProfile.getCellProfileByStableId(cellProfileId);
            if (cellProfile == null) {
                buf.append("No cell profile available for " + WebService.CELL_PROFILE_ID + ":  ")
                        .append(cellProfileId).append(".").append (WebApiUtil.NEW_LINE);
                return buf.toString();
            }
        }
        // validate all cell profiles belong to the same cancer study
        if (differentCancerStudies(targetCellProfileIdList)) {
            buf.append("Cell profiles must come from same cancer study.").append((WebApiUtil.NEW_LINE));
            return buf.toString();
        }
        int cancerStudyId = DaoCellProfile.getCellProfileByStableId(targetCellProfileIdList.get(0)).getCancerStudyId();
        List<Integer> internalSampleIds = InternalIdUtil.getInternalSampleIds(cancerStudyId, targetSampleList);

        //  Branch based on number of profiles requested.
        //  In the first case, we have 1 profile and 1 or more cells.
        //  In the second case, we have > 1 profiles and only 1 cell.
        if (targetCellProfileIdList.size() == 1) {
            String cellProfileId = targetCellProfileIdList.get(0);
            CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId(cellProfileId);

            //  Get the Cell List
            List<Cell> cellList = WebApiUtil.getCellList(targetCellList,
                    cellProfile.getCellAlterationType(), buf, warningList);
            
            //  Output DATA_TYPE and COLOR_GRADIENT_SETTINGS (Used by Mondrian Cytoscape PlugIn)
            if (!suppressMondrianHeader) {
                buf.append("# DATA_TYPE\t ").append(cellProfile.getProfileName()).append ("\n");
                buf.append("# COLOR_GRADIENT_SETTINGS\t ").append(cellProfile.getCellAlterationType().name())
                        .append ("\n");
            }

            //  Ouput Column Headings
            buf.append ("CELL_ID\tCOMMON");
            outputRow(targetSampleList, buf);

            //  Iterate through all validated cells, and extract profile data.
            for (Cell cell: cellList) {                
                List<String> dataRow = CellAlterationUtil.getCellAlterationDataRow(cell,
                        internalSampleIds, cellProfile);
                outputCellRow(dataRow, cell, buf);
            }
        } else {
            //  Ouput Column Headings
            buf.append ("CELL_PROFILE_ID\tALTERATION_TYPE\tCELL_ID\tCOMMON");
            outputRow(targetSampleList, buf);
            
            List<CellProfile> profiles = new ArrayList<CellProfile>(targetCellProfileIdList.size());
            boolean includeRPPAProteinLevel = false;
            for (String gId:  targetCellProfileIdList) {
                CellProfile profile = DaoCellProfile.getCellProfileByStableId(gId);
                profiles.add(profile);
            }

            //  Iterate through all cell profiles
            for (CellProfile cellProfile : profiles) {
                //  Get the Cell List
                List<Cell> cellList = WebApiUtil.getCellList(targetCellList,
                        cellProfile.getCellAlterationType(), buf, warningList);

                if (cellList.size() > 0) {
                    Cell cell = cellList.get(0);
                    buf.append(cellProfile.getStableId()).append(WebApiUtil.TAB)
                            .append(cellProfile.getCellAlterationType().name()).append(WebApiUtil.TAB);   
                    List<String> dataRow = CellAlterationUtil.getCellAlterationDataRow(cell,
                            internalSampleIds, cellProfile);
                    outputCellRow(dataRow, cell, buf);
                }
            }
        }
        return buf.toString();
    }

    private static boolean differentCancerStudies(List<String> targetCellProfileList)
    {
        if (targetCellProfileList.size() == 1) return false;

        int firstCancerStudyId = -1;
        boolean processingFirstId = true;
        for (String profileId : targetCellProfileList) {
            CellProfile p = DaoCellProfile.getCellProfileByStableId(profileId);
          if (processingFirstId) {
                firstCancerStudyId = p.getCancerStudyId();
                processingFirstId = false;
          }  
          else if (p.getCancerStudyId() != firstCancerStudyId) {
              return true;
          }
        }
        return false;
    }

    private static void outputRow(List<String> dataValues, StringBuffer buf) {
        for (String value:  dataValues) {
            buf.append(WebApiUtil.TAB).append (value);
        }
        buf.append (WebApiUtil.NEW_LINE);
    }

    private static void outputCellRow(List<String> dataRow, Cell cell, StringBuffer buf)
            throws DaoException {
        if (cell instanceof CanonicalCell) {
            CanonicalCell canonicalCell = (CanonicalCell) cell;
            buf.append(canonicalCell.getUniqueCellId()).append (WebApiUtil.TAB);
            buf.append (canonicalCell.getUniqueCellNameAllCaps());
        }
        outputRow (dataRow, buf);
    }

    public JSONArray getJson() {
        JSONArray toReturn = new JSONArray();

        List<String> sampleIds = new ArrayList<String>(Arrays.asList(matrix[0]));
        sampleIds.subList(0,4).clear();       // remove column names and meta data

        for (String s : sampleIds) {
            toReturn.add(s);
        }

        return toReturn;
    }
}
