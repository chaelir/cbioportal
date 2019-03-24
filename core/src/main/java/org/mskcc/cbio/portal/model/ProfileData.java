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

package org.mskcc.cbio.portal.model;

import org.mskcc.cbio.portal.util.ValueParser;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Encapsulates Genetic Profile Data.
 * Stores the properties of each gene in each case.
 * Also stores lists of the genes and cases.
 *
 * @author Ethan Cerami.
 */
public class ProfileData {
    private String[][] matrix;
    private GeneticProfile geneticProfile;
    
    /* NOTE: BEGIN OF HACK */
    private CellProfile cellProfile;
    /* NOTE: END OF HACK */
    
    // primary store of genetic profile's data:
    /* NOTE: BEGIN OF HACK */
    // apply to all profiles
    //private HashMap<String, String> mapFromNameAndCaseToProfileProperties = new HashMap<String, String>();
    private HashMap<String, String> mapFromNameAndCaseToProfileProperties = new HashMap<String, String>();
    private ArrayList<String> nameList = new ArrayList<String>();
    /* NOTE: END OF HACK */
    private ArrayList<String> caseIdList = new ArrayList<String>();
    
    /**
     * Constructor.
     *
     * @param geneticProfile GeneticProfile Object.
     * @param matrix         2D Matrix of Data
     */
    public ProfileData(GeneticProfile geneticProfile, String[][] matrix) {
        this.geneticProfile = geneticProfile;
        this.matrix = matrix;
        processMatrix();
    }

    /* NOTE: BEGIN OF HACK */
    /** 
     * Constructor.
     *
     * @param cellProfile CellProfile Object.
     * @param matrix         2D Matrix of Data
     */
    public ProfileData(CellProfile cellProfile, String[][] matrix) {
        this.cellProfile = cellProfile;
        this.matrix = matrix;
        processMatrix();
    }
    /* NOTE: END OF HACK */

    /**
     * Constructor.
     *
     * @param hashMap    HashMap of Data.
     * @param nameList   List of Names [ Cells or Genes ]
     * @param caseIdList List of Case Ids.
     */
    public ProfileData(HashMap<String, String> hashMap,
                       ArrayList<String> nameList, ArrayList<String> caseIdList) {
        this.mapFromNameAndCaseToProfileProperties = hashMap;
        this.nameList = nameList;
        this.caseIdList = caseIdList;
    }

    /**
     * Gets the Data Matrix.
     *
     * @return 2D Matrix of Data.
     */
    public String[][] getMatrix() {
        return matrix;
    }

    /**
     * Gets the Genetic Profile.
     *
     * @return Genetic Profile Object.
     */
    public GeneticProfile getGeneticProfile() {
        return geneticProfile;
    }
    
    /* NOTE: BEGIN OF HACK */
    /**
     * Gets the Cell Profile.
     *
     * @return Cell Profile Object.
     */
    public CellProfile getCellProfile() {
        return cellProfile; 
    }
    /* NOTE: END OF HACK */

    /**
     * Gets the value of gene X in case Y.
     *
     * @param nameSymbol Unique Name Symbol.
     * @param caseId     Case ID.
     * @return value.
     */
    public String getValue(String nameSymbol, String caseId) {
        String key = createKey(nameSymbol, caseId);
        return mapFromNameAndCaseToProfileProperties.get(key);
    }

    /**
     * Gets the value of gene X in case Y.
     *
     * @param nameSymbol Unique Name Symbol.
     * @param caseId     Case ID.
     * @return value.
     */
    public ValueParser getValueParsed(String nameSymbol, String caseId, double zScoreThreshold) {
        String key = createKey(nameSymbol, caseId);
        String value = mapFromNameAndCaseToProfileProperties.get(key);
        if (value != null) {
            return new ValueParser (value, zScoreThreshold);
        }
        return null;
    }

    /**
     * Gets list of case Ids.
     *
     * @return ArrayList of Case IDs.
     */
    public ArrayList<String> getCaseIdList() {
        return caseIdList;
    }

    /**
     * Gets list of gene symbols.
     *
     * @return ArrayList of Gene Symbols.
     */
    public ArrayList<String> getGeneList() {
        return nameList;
    }
    
    /**
     * Gets list of cell symbols.
     *
     * @return ArrayList of Cell Symbols.
     */
    public ArrayList<String> getCellList() {
        return nameList;
    }

    /**
     * Process the data matrix.
     */
    private void processMatrix() {
        //  First, extract the case IDs
        if (matrix[0].length > 0) {
            for (int cols = 2; cols < matrix[0].length; cols++) {
                String caseId = matrix[0][cols];
                caseIdList.add(caseId);
            }
        }

        //  Then, extract the gene list
        if (matrix.length > 0) {
            for (int rows = 1; rows < matrix.length; rows++) {
                String nameSymbol = matrix[rows][1];
                nameList.add(nameSymbol);
            }
        }

        //  Then, store to hashtable for quick look up
        for (int rows = 1; rows < matrix.length; rows++) {
            for (int cols = 2; cols < matrix[0].length; cols++) {
                String value = matrix[rows][cols];
                String caseId = matrix[0][cols];
                String nameSymbol = matrix[rows][1];
                String key = createKey(nameSymbol, caseId);
                mapFromNameAndCaseToProfileProperties.put(key, value);
            }
        }
    }

    /**
     * Create name + case ID key.
     *
     * @param nameSymbol unique name symbol.
     * @param caseId     case ID.
     * @return hash key.
     */
    private String createKey(String nameSymbol, String caseId) {
        return nameSymbol + ":" + caseId;
    }
}
