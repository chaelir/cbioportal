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

package org.mskcc.cbio.portal.scripts;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

import joptsimple.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.*;

/**
 * Import 'profile' files that contain data matrices indexed by cell, case.
 *
 * @author ECerami
 * @author Arthur Goldberg goldberg@cbio.mskcc.org
 */
public class ImportCellProfileData extends ConsoleRunnable {

    public void run() {
        try {
            String description = "Import 'cell profile' files that contain data matrices indexed by cell, case";
            // using a real options parser, helps avoid bugs

            OptionSet options = ConsoleUtil.parseStandardDataAndMetaOptions(args, description, true);
            File dataFile = new File((String) options.valueOf("data"));
            File descriptorFile = new File((String) options.valueOf( "meta" ) );
            SpringUtil.initDataSource();
            ProgressMonitor.setCurrentMessage("Reading data from:  " + dataFile.getAbsolutePath());
            CellProfile cellProfile = null;
            String cellPanel = null; // a null cellPanle won't mess up
            /* NOTE: disabled mutation and cell panel stuff 
            Set<String> filteredMutations = CellProfileReader.getVariantClassificationFilter( descriptorFile );
            */
            try {
                cellProfile = CellProfileReader.loadCellProfile( descriptorFile );
                /* NOTE: disabled mutation and cell panel stuff
                cellPanel = CellProfileReader.loadCellPanelInformation( descriptorFile );
                */
            } catch (java.io.FileNotFoundException e) {
                throw new java.io.FileNotFoundException("Descriptor file '" + descriptorFile + "' not found.");
            }
            int numLines = FileUtil.getNumLines(dataFile);
            ProgressMonitor.setCurrentMessage(
                    " --> cell profile id:  " + cellProfile.getCellProfileId() +
                    "\n --> cell profile name:  " + cellProfile.getProfileName() +
                    "\n --> cell alteration type:  " + cellProfile.getCellAlterationType().name());
            ProgressMonitor.setMaxValue(numLines);
            /* NOTE: disabled mutation and fusion stuff
            if (cellProfile.getCellAlterationType() == CellAlterationType.MUTATION_EXTENDED || 
                cellProfile.getCellAlterationType() == CellAlterationType.MUTATION_UNCALLED) {
                ImportExtendedMutationData importer = new ImportExtendedMutationData(dataFile, cellProfile.getCellProfileId(), cellPanel, filteredMutations);
                String swissprotIdType = cellProfile.getOtherMetaDataField("swissprot_identifier");
                if (swissprotIdType != null && swissprotIdType.equals("accession")) {
                    importer.setSwissprotIsAccession(true);
                } else if (swissprotIdType != null && !swissprotIdType.equals("name")) {
                    throw new RuntimeException( "Unrecognized swissprot_identifier specification, must be 'name' or 'accession'.");
                }
                importer.importData();
            } else if (cellProfile.getCellAlterationType() == CellAlterationType.FUSION) {
                ImportFusionData importer = new ImportFusionData(dataFile, cellProfile.getCellProfileId(), cellPanel);
                importer.importData();
            } else {
                ImportTabDelimData importer = new ImportTabDelimData(dataFile, cellProfile.getTargetLine(), cellProfile.getCellProfileId(), cellPanel);
                importer.importData(numLines);
            }
            */
            ImportTabDelimData importer = new ImportTabDelimData(dataFile, cellProfile.getTargetLine(), cellProfile.getCellProfileId(), cellPanel);
            importer.importData(numLines);
       }
       catch (Exception e) {
    	   e.printStackTrace();
           throw new RuntimeException(e);
       }
    }

    /**
     * Makes an instance to run with the given command line arguments.
     *
     * @param args  the command line arguments to be used
     */
    public ImportProfileData(String[] args) {
        super(args);
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args  the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new ImportProfileData(args);
        runner.runInConsole();
    }
}