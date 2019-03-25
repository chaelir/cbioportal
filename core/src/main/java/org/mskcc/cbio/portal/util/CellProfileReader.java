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

package org.mskcc.cbio.portal.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.CellAlterationType;
import org.mskcc.cbio.portal.model.CellProfile;
import org.mskcc.cbio.portal.model.CellProfileLink;
import org.mskcc.cbio.portal.scripts.TrimmedProperties;

/**
 * Prepare a CellProfile for having its data loaded.
 *
 * @author Ethan Cerami
 * @author Arthur Goldberg goldberg@cbio.mskcc.org
 */
public class CellProfileReader {

    /**
     * Load a CellProfile. Get a stableID from a description file. If the same
     * CellProfile already exists in the dbms use it, otherwise create a new
     * CellProfile dbms record, defining all parameters from the file.
     *
     * @author Ethan Cerami
     * @author Arthur Goldberg goldberg@cbio.mskcc.org
     *
     * @param file
     *           A handle to a description of the cell profile, i.e., a
     *           'description' or 'meta' file.
     * @return an instantiated CellProfile record
     * @throws IOException
     *            if the description file cannot be read
     * @throws DaoException
     */
    public static CellProfile loadCellProfile(File file) throws IOException, DaoException {
        CellProfile cellProfile = loadCellProfileFromMeta(file);
        CellProfile existingCellProfile = DaoCellProfile.getCellProfileByStableId(cellProfile.getStableId());
        
        // first, try returning existing
        if (existingCellProfile != null) {
            /* NOTE Disabled Mutation Stuff
            if (!existingCellProfile.getDatatype().equals("MAF")) {
               // the dbms already contains a CellProfile with the file's stable_id. This scenario is not supported
               // anymore, so throw error telling user to remove existing profile first:
               throw new RuntimeException("Error: IM_cell_profile record found with same Stable ID as the one used in your data:  "
                       + existingCellProfile.getStableId() + ". Remove the existing IM_cell_profile record first.");
            } else if (cellProfile.getDatatype().equals("FUSION")) {
                cellProfile.setCellProfileId(existingCellProfile.getCellProfileId());
                return cellProfile;
            } else {
                // For mutation data only we can have multiple files with the same IM_cell_profile.
                // There is a constraint in the mutation database table to prevent duplicated data
                // If this constraint is hit (mistakenly importing the same maf twice) MySqlBulkLoader will throw an exception
                //
                // make an object combining the pre-existing profile with the file-specific properties of the current file
                CellProfile gp = new CellProfile(existingCellProfile);
                gp.setTargetLine(gp.getTargetLine());
                gp.setOtherMetadataFields(gp.getAllOtherMetadataFields());
                return gp;
            }
            */
            CellProfile gp = new CellProfile(existingCellProfile);
            gp.setTargetLine(gp.getTargetLine());
            gp.setOtherMetadataFields(gp.getAllOtherMetadataFields());
            return gp;
        }

        CellProfileLink cellProfileLink = null;
        
        /* NOTE: diabled GSVA stuff
        // For GSVA profiles, we want to create a cellProfileLink from source_stable_id for:
        // - expression zscores -> expression
        // - gsva scores -> expression
        // - gsva pvalues -> gsva scores
        // Currently we only create cellProfileLink for expression zscores when it's available. 
        // This cellProfileLink is required for the oncoprint in a GSVA study. In the future it might
        // be useful to make this a requirement for every expression zscore file.
    	if (cellProfile.getCellAlterationType() == CellAlterationType.GENESET_SCORE ||
    			(cellProfile.getCellAlterationType() == CellAlterationType.MRNA_EXPRESSION && 
    			cellProfile.getDatatype().equals("Z-SCORE") && 
    			cellProfile.getAllOtherMetadataFields().getProperty("source_stable_id") != null)) {
            cellProfileLink = createCellProfileLink(cellProfile);
    	}

		// For GSVA profiles, we want to check that the version in the meta file is
        // the same as the version of the cell sets in the database (cellsets_info table).
    	if (cellProfile.getCellAlterationType() == CellAlterationType.GENESET_SCORE) {
            validateCellsetProfile(cellProfile, file);
    	}
    	*/

        // second, add new cell profile
        DaoCellProfile.addCellProfile(cellProfile);
        	
        // add cell profile link if set
        if (cellProfileLink != null) {
            // Set `REFERRING_CELL_PROFILE_ID`
        	int cellProfileId = DaoCellProfile.getCellProfileByStableId(cellProfile.getStableId()).getCellProfileId();
            cellProfileLink.setReferringCellProfileId(cellProfileId);
            DaoCellProfileLink.addCellProfileLink(cellProfileLink);
        }
        
        // Get ID
        CellProfile gp = DaoCellProfile.getCellProfileByStableId(cellProfile.getStableId());
        cellProfile.setCellProfileId(gp.getCellProfileId());
        return cellProfile;
    }

    private static CellProfileLink createCellProfileLink(CellProfile cellProfile) {
    	CellProfileLink cellProfileLink = new CellProfileLink();
        
        // Set `REFERRED_CELL_PROFILE_ID`
        String referredCellProfileStableId = parseStableId(cellProfile.getAllOtherMetadataFields(), "source_stable_id");
        if (referredCellProfileStableId == null) {
        	throw new RuntimeException("'source_stable_id' is required in meta file for " + cellProfile.getStableId());
        }
        CellProfile referredCellProfile = DaoCellProfile.getCellProfileByStableId(referredCellProfileStableId);
        cellProfileLink.setReferredCellProfileId(referredCellProfile.getCellProfileId());

        // Decide reference type
        // In the future with other types of cell profile links, this should be configurable in the meta file. 
        String referenceType;
        if (Arrays.asList("P-VALUE", "Z-SCORE").contains(cellProfile.getDatatype())) {
        	referenceType = "STATISTIC";
        } else if (cellProfile.getDatatype().equals("GSVA-SCORE")) {
        	referenceType = "AGGREGATION";
        } else {
        	// not expected but might be useful for future cell profile links
        	throw new RuntimeException("Unknown datatype '" + cellProfile.getDatatype() + "' in meta file for " + cellProfile.getStableId());
        }
        // Set `REFERENCE_TYPE`
        cellProfileLink.setReferenceType(referenceType);
        
        return cellProfileLink;
	}

	/* NOTE: disabled Cellset functions
	private static void validateCellsetProfile(CellProfile cellProfile, File file) throws DaoException {
    	String cellsetVersion = DaoInfo.getCellsetVersion();
    	
    	// TODO Auto-produced method stub
    	
    	// Check if version is present in database
      if (cellsetVersion == null) {
         throw new RuntimeException("Attempted to import GENESET_SCORE data, but all cell set tables are empty.\n"
            + "Please load cell sets with ImportCellsetData.pl first. See:\n"
            + "https://github.com/cBioPortal/cbioportal/blob/master/docs/Import-Cell-Sets.md\n");

    		// Check if version is present in meta file
    	} else if (cellProfile.getOtherMetaDataField("cellset_def_version") == null) {
    		throw new RuntimeException("Missing cellset_def_version property in '" + file.getPath() + "'. This version must be "
    				+ "the same as the cell set version loaded with ImportCellsetData.pl .");

    		// Check if version is same as database version
    	} else if (!cellProfile.getOtherMetaDataField("cellset_def_version").equals(cellsetVersion)) {
    		throw new RuntimeException("'cellset_def_version' property (" + cellProfile.getOtherMetaDataField("cellset_def_version") +
    				") in '" + file.getPath() + "' differs from database version (" + cellsetVersion + ").");
    	}

    	// Prevent p-value profile to show up as selectable genomic profile
    	if (cellProfile.getDatatype().equals("P-VALUE")) {
    		cellProfile.setShowProfileInAnalysisTab(false);
    	}
    }
    */

	/**
     * Load a CellProfile from a description file.
     *
     * @author Ethan Cerami
     * @author Arthur Goldberg goldberg@cbio.mskcc.org
     *
     * @param file
     *           A handle to a description of the cell profile, i.e., a
     *           'description' or 'meta' file.
     * @return an instantiated CellProfile
     * @throws IOException
     *            if the description file cannot be read
     * @throws DaoException
     */
    public static CellProfile loadCellProfileFromMeta(File file) throws IOException, DaoException {
        Properties properties = new TrimmedProperties();
        properties.load(new FileInputStream(file));
        // when loading cancer studies and their profiles from separate files,
        // use the cancer_study_identifier as a unique id for each study.
        // this was called the "cancer_type_id" previously.
        // eventually, it won't be needed when studies are loaded by a connected client that
        // knows its study_id in its state
        String cancerStudyIdentifier = properties.getProperty("cancer_study_identifier");
        if (cancerStudyIdentifier == null) {
            throw new IllegalArgumentException("cancer_study_identifier is not specified.");
        }
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId(cancerStudyIdentifier);
        if (cancerStudy == null) {
            throw new IllegalArgumentException("cancer study identified by cancer_study_identifier " + cancerStudyIdentifier + " not found in dbms.");
        }
        String stableId = parseStableId(properties, "stable_id");
        String profileName = properties.getProperty("profile_name");
        String profileDescription = properties.getProperty("profile_description");
        // NOTE: this terminology is techinically wrong
        // BUT will keep it this way to ease coding. see cbioportal_common.py
        String cellAlterationTypeString = properties.getProperty("genetic_alteration_type");
        String datatype = properties.getProperty("datatype");
        if (profileName == null) {
            profileName = cellAlterationTypeString;
        }
        if (profileDescription == null) {
            profileDescription = cellAlterationTypeString;
        }
        if (cellAlterationTypeString == null) {
            throw new IllegalArgumentException("cell_alteration_type is not specified.");
        } else if (datatype == null) {
            datatype = "";
        }
        boolean showProfileInAnalysisTab = true;
        String showProfileInAnalysisTabStr = properties.getProperty("show_profile_in_analysis_tab");
        if (showProfileInAnalysisTabStr != null && showProfileInAnalysisTabStr.equalsIgnoreCase("FALSE")) {
            showProfileInAnalysisTab = false;
        }

        profileDescription = profileDescription.replaceAll("\t", " ");
        CellAlterationType alterationType = CellAlterationType.valueOf(cellAlterationTypeString);
        CellProfile cellProfile = new CellProfile();
        cellProfile.setCancerStudyId(cancerStudy.getInternalId());
        cellProfile.setStableId(stableId);
        cellProfile.setProfileName(profileName);
        cellProfile.setProfileDescription(profileDescription);
        cellProfile.setCellAlterationType(alterationType);
        cellProfile.setDatatype(datatype);
        cellProfile.setShowProfileInAnalysisTab(showProfileInAnalysisTab);
        cellProfile.setTargetLine(properties.getProperty("target_line"));
        cellProfile.setOtherMetadataFields(properties);
        return cellProfile;
    }

    private static String parseStableId(Properties properties, String stableIdPropName) {
        String stableId = properties.getProperty(stableIdPropName);
        if (stableId == null) {
            throw new IllegalArgumentException("stable_id is not specified.");
        }
        String cancerStudyIdentifier = properties.getProperty("cancer_study_identifier");
        //automatically add the cancerStudyIdentifier in front of stableId (since the rest of the
        //code still relies on this - TODO: this can be removed once the rest of the backend and frontend code
        //stop assuming cancerStudyIdentifier to be part of stableId):
        if (!stableId.startsWith(cancerStudyIdentifier + "_")) {
            stableId = cancerStudyIdentifier + "_" + stableId;
        }
        
        /* NOTE: disabled fusion and mutation stuff
        // Workaround to import fusion data as mutation cell profile. This way fusion meta file can contain 'stable_id: fusion'.
        // The validator will check for 'stable_id: fusion', and this section in the importer
        // will convert it to 'stable_id: mutations'. See https://github.com/cBioPortal/cbioportal/pull/2506
        // TODO: This should be removed when other parts of cBioPortal have implemented support for a separate fusion profile".
        if (stableId.equals(cancerStudyIdentifier + "_fusion")) {
            String newStableId = cancerStudyIdentifier + "_mutations";
            CellProfile existingCellProfile = DaoCellProfile.getCellProfileByStableId(newStableId);
            if (existingCellProfile == null) {
                throw new IllegalArgumentException("Wrong order: FUSION data should be loaded after MUTATION data");
            }
            stableId = newStableId;
        }
        */
        
        return stableId;
	}


    /* NOTE: disabled Cell Panel stuff

    public static String loadCellPanelInformation(File file) throws Exception {
        Properties properties = new TrimmedProperties();
        properties.load(new FileInputStream(file));
        return properties.getProperty("cell_panel");
    }
    */

    /**
    * Gets the information of "variant_classification_filter" in the file, if it exists. Otherwise, it
    * returns null. "variant_classification_filter" can be used in the mutation meta file to specify
    * which types of mutations want to be filtered.
    * 
    * @param file
    * @return a string with the types of mutations that should be filtered, comma-separated.
    * @throws Exception
    */
    /* NOTE: disabled mutation stuff
	public static Set<String> getVariantClassificationFilter(File file) throws Exception {
	    Properties properties = new TrimmedProperties();
	    properties.load(new FileInputStream(file));
	    String variantClassificationFilter = properties.getProperty("variant_classification_filter");
	    if (variantClassificationFilter != null) {
		    Set<String> filteredMutations = new HashSet<String>();
		    for (String mutation : (Arrays.asList(variantClassificationFilter.split(",")))) {
		            mutation = mutation.trim();
		            filteredMutations.add(mutation);
		        }
		    return filteredMutations;
	    } else {
		return null;
	    }
	}
	*/
}
