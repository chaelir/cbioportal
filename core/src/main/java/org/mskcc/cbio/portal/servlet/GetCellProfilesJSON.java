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

package org.mskcc.cbio.portal.servlet;

import java.io.*;
import java.util.*;
import javax.servlet.ServletException;
import javax.servlet.http.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.*;

/**
 * Get the cell profiles for a cancer study
 *
 * same input and output as the original web API
 * getCellProfiles
 * except return JSON instead of plain text
 *
 * @param cancer_study_id
 * @param case_set_id, case_ids_key, cell_list (optional)
 * @return JSON objects of cell profiles
 */
public class GetCellProfilesJSON extends HttpServlet  {

	// class which process access control to cancer studies
    private AccessControl accessControl;
    
    /**
     * Initializes the servlet.
     */
    public void init() throws ServletException {
        super.init();
        accessControl = SpringUtil.getAccessControl();
    }
    
    /**
     * Handles HTTP GET Request.
     *
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws ServletException
     */
    protected void doGet(HttpServletRequest httpServletRequest,
                         HttpServletResponse httpServletResponse) throws ServletException, IOException {
        doPost(httpServletRequest, httpServletResponse);
    }

    /**
     * Handles the HTTP POST Request.
     *
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws ServletException
     */
    protected void doPost(HttpServletRequest httpServletRequest,
                          HttpServletResponse httpServletResponse) throws ServletException, IOException {

        String cancerStudyIdentifier = httpServletRequest.getParameter("cancer_study_id");
        String sampleSetId = httpServletRequest.getParameter("case_set_id");
        String sampleIdsKey = httpServletRequest.getParameter("case_ids_key");
        String cellListStr = httpServletRequest.getParameter("cell_list");
        if (httpServletRequest instanceof XssRequestWrapper) {
            cellListStr = ((XssRequestWrapper)httpServletRequest).getRawParameter("cell_list");
        }

		CancerStudy cancerStudy = null;
		try {
			if (cancerStudyIdentifier != null) {
				cancerStudy = DaoCancerStudy.getCancerStudyByStableId(cancerStudyIdentifier);
				if (cancerStudy == null
						|| accessControl.isAccessibleCancerStudy(cancerStudy.getCancerStudyStableId()).size() == 0) {
					return;
				}
			} else {
				return;
			}
		} catch (DaoException e) {
			System.out.println("DaoException Caught:" + e.getMessage());
			return;
		}
		
        if (cancerStudy != null) {

            int cancerStudyId = cancerStudy.getInternalId();

            JSONObject result = new JSONObject();
            ArrayList<CellProfile> list =
                    DaoCellProfile.getAllCellProfiles(cancerStudyId);

            if (list.size() > 0) {
                //Retrieve all the profiles available for this cancer study
                if (sampleSetId == null && cellListStr == null) {
                    for (CellProfile cellProfile : list) {
                        JSONObject tmpProfileObj = new JSONObject();
                        tmpProfileObj.put("STABLE_ID", cellProfile.getStableId());
                        tmpProfileObj.put("NAME", cellProfile.getProfileName());
                        tmpProfileObj.put("DESCRIPTION", cellProfile.getProfileDescription());
                        tmpProfileObj.put("CELL_ALTERATION_TYPE", cellProfile.getCellAlterationType().name());
                        tmpProfileObj.put("CANCER_STUDY_ID", cellProfile.getCancerStudyId());
                        tmpProfileObj.put("SHOW_PROFILE_IN_ANALYSIS_TAB", cellProfile.showProfileInAnalysisTab());
                        // added datatype to be able to make distinction between log data and non-log data
                        tmpProfileObj.put("DATATYPE", cellProfile.getDatatype());
                        result.put(cellProfile.getStableId(), tmpProfileObj);
                    }
                    httpServletResponse.setContentType("application/json");
                    PrintWriter out = httpServletResponse.getWriter();
                    JSONValue.writeJSONString(result, out);
                } else if (cellListStr != null && sampleSetId != null && sampleIdsKey != null) { //Only return data available profiles for each queried cell
                    String[] cellList = cellListStr.split("\\s+");
                    try {
                        //Get patient ID list
                        DaoSampleList daoSampleList = new DaoSampleList();
                        SampleList sampleList;
                        ArrayList<String> sampleIdList = new ArrayList<String>();
                        if (sampleSetId.equals("-1") && sampleIdsKey.length() != 0) {
                            String strSampleIds = SampleSetUtil.getSampleIds(sampleIdsKey);
                            String[] sampleArray = strSampleIds.split("\\s+");
                            for (String item : sampleArray) {
                                sampleIdList.add(item);
                            }
                        } else {
                            sampleList = daoSampleList.getSampleListByStableId(sampleSetId);
                            sampleIdList = sampleList.getSampleList();
                        }
                        // NOTE - as of 12/12/14, patient lists contain sample ids
                        List<Integer> internalSampleIds = InternalIdUtil.getInternalNonNormalSampleIds(cancerStudyId, sampleIdList);

                        for (String cellId : cellList) {
                            //Get cell
                            DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
                            Cell cell = daoCell.getCell(cellId);

                            JSONObject tmpResult = new JSONObject();
                            for (CellProfile cellProfile : list) {
                                ArrayList<String> tmpProfileDataArr = CellAlterationUtil.getCellAlterationDataRow(
                                        cell,
                                        internalSampleIds,
                                        DaoCellProfile.getCellProfileByStableId(cellProfile.getStableId()));
                                if (isDataAvailable(tmpProfileDataArr)) {
                                    JSONObject tmpProfileObj = new JSONObject();
                                    tmpProfileObj.put("STABLE_ID", cellProfile.getStableId());
                                    tmpProfileObj.put("NAME", cellProfile.getProfileName());
                                    tmpProfileObj.put("DESCRIPTION", cellProfile.getProfileDescription());
                                    tmpProfileObj.put("CELL_ALTERATION_TYPE", cellProfile.getCellAlterationType().name());
                                    tmpProfileObj.put("CANCER_STUDY_ID", cellProfile.getCancerStudyId());
                                    tmpProfileObj.put("SHOW_PROFILE_IN_ANALYSIS_TAB", cellProfile.showProfileInAnalysisTab());
                                    // added datatype to be able to make distinction between log data and non-log data
                                    tmpProfileObj.put("DATATYPE", cellProfile.getDatatype());
                                    tmpResult.put(cellProfile.getStableId(), tmpProfileObj);
                                }
                            }
                            result.put(cellId, tmpResult);
                        }
                    } catch (DaoException e) {
                        System.out.println("DaoException Caught:" + e.getMessage());
                    }
                    httpServletResponse.setContentType("application/json");
                    PrintWriter out = httpServletResponse.getWriter();
                    JSONValue.writeJSONString(result, out);
                } else {
                    httpServletResponse.setContentType("application/text");
                    PrintWriter out = httpServletResponse.getWriter();
                    out.print("Error: Please provide both CELL ID and CASE_SET_ID/CASE_IDS_KEY");
                    out.flush();
                }
            } else {
                httpServletResponse.setContentType("application/text");
                PrintWriter out = httpServletResponse.getWriter();
                out.print("Error:  No cell profiles available for: " + cancerStudyId);
                out.flush();
            }

        }
    }

    private boolean isDataAvailable(ArrayList<String> inputArr) {
        if (inputArr.size() == 0) return false;
        for (String item : inputArr) {
            if (item != null && item != "NaN" && item != "NA") {
                return true;
            }
        }
        return false;
    }

}
