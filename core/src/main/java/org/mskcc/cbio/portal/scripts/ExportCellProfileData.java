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

package org.mskcc.cbio.portal.scripts;

import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.*;

import java.io.*;
import java.util.*;

/**
 * Export all Data Associated with a Single Genomic Profile.
 *
 * @author Ethan Cerami.
 */
public class ExportCellProfileData {
    private static final String TAB = "\t";
    private static final String NEW_LINE = "\n";

    public static void main(String[] args) throws DaoException, IOException {
        // check args
        if (args.length < 1) {
            System.out.println("command line usage:  exportProfileData.pl " + "<stable_cell_profile_id>");
            // an extra --noprogress option can be given to avoid the messages regarding memory usage and % complete
            return;
        }
        String stableCellProfileId = args[0];
        System.out.println("Using cell profile ID:  " + stableCellProfileId);
        CellProfile cellProfile = DaoCellProfile.getCellProfileByStableId(stableCellProfileId);
        if (cellProfile == null) {
            System.out.println("Cell Profile not recognized:  " + stableCellProfileId);
            return;
        } else {
            System.out.println(cellProfile.getProfileName());
            ProgressMonitor.setConsoleModeAndParseShowProgress(args);
            export(cellProfile);
        }
    }

    public static void export(CellProfile profile) throws IOException, DaoException {
        String fileName = profile.getStableId() + ".txt";
        FileWriter writer = new FileWriter (fileName);
        ArrayList<Integer> sampleList = outputHeader(profile, writer);

        DaoCellAlteration daoCellAlteration = DaoCellAlteration.getInstance();
        Set<CanonicalCell> cellSet = daoCellAlteration.getCellsInProfile(profile.getCellProfileId());
        ProgressMonitor.setMaxValue(cellSet.size());
        Iterator<CanonicalCell> cellIterator = cellSet.iterator();
        outputProfileData(profile, writer, sampleList, daoCellAlteration, cellIterator);
        System.out.println ("\nCell Profile data written to:  " + fileName);
    }

    private static void outputProfileData(CellProfile profile, FileWriter writer,
            ArrayList<Integer> sampleList, DaoCellAlteration daoCellAlteration,
            Iterator<CanonicalCell> cellIterator) throws IOException, DaoException {
        while (cellIterator.hasNext()) {
            ConsoleUtil.showProgress();
            ProgressMonitor.incrementCurValue();
            CanonicalCell currentCell = cellIterator.next();
            writer.write(currentCell.getUniqueCellNameAllCaps() + TAB);
            writer.write(Long.toString(currentCell.getUniqueCellId()));
            HashMap<Integer, String> valueMap = daoCellAlteration.getCellAlterationMap
                    (profile.getCellProfileId(), currentCell.getUniqueCellId());
            for (Integer sampleId:  sampleList) {
                writer.write(TAB + valueMap.get(sampleId));
            }
            writer.write(NEW_LINE);
        }
        writer.close();
    }

    private static ArrayList<Integer> outputHeader(CellProfile profile, FileWriter writer) throws DaoException, IOException {
        ArrayList<Integer> sampleList = DaoCellProfileSamples.getOrderedSampleList(profile.getCellProfileId());
        writer.write("UNIQUE_CELL_NAME" + TAB);
        writer.write("UNIQUE_CELL_ID");
        for (Integer sampleId : sampleList) {
            Sample s = DaoSample.getSampleById(sampleId);
            writer.write(TAB + s.getStableId());
        }
        writer.write(NEW_LINE);
        return sampleList;
    }
}