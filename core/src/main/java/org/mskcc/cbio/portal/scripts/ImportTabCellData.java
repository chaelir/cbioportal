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

import org.apache.commons.lang.ArrayUtils;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.util.ConsoleUtil;
import org.mskcc.cbio.portal.util.ImportDataUtil;
import org.mskcc.cbio.portal.util.ProgressMonitor;
import org.mskcc.cbio.portal.util.StableIdUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Code to Import Cell Relative Abundance data
 *
 * @author Ethan Cerami
 */
public class ImportTabCellData {
    /*
    public static final String CNA_VALUE_AMPLIFICATION = "2";
    public static final String CNA_VALUE_GAIN = "1";
    public static final String CNA_VALUE_HEMIZYGOUS_DELETION = "-1";
    public static final String CNA_VALUE_HOMOZYGOUS_DELETION = "-2";
    public static final String CNA_VALUE_PARTIAL_DELETION = "-1.5";
    public static final String CNA_VALUE_ZERO = "0";
    */
    private HashSet<Long> importSetOfCells = new HashSet<Long>();
    private HashSet<Integer> importedCellEntitySet = new HashSet<>(); 
    private File dataFile;
    private String targetLine;
    private int cellProfileId;
    private CellProfile cellProfile;
    private int entriesSkipped = 0;
    private int nrExtraRecords = 0;
    private Set<String> arrayIdSet = new HashSet<String>();
    private String cellPanelID;

    /**
     * Constructor.
     *
     * @param dataFile         Data File containing Copy Number Alteration, MRNA Expression Data, or protein RPPA data
     * @param targetLine       The line we want to import.
     *                         If null, all lines are imported.
     * @param cellProfileId    CellProfile ID.
     * 
     * @deprecated : TODO shall we deprecate this feature (i.e. the targetLine)? 
     */
    public ImportTabCellData(File dataFile, String targetLine, int cellProfileId, String cellPanelID) {
        this.dataFile = dataFile;
        this.targetLine = targetLine;
        this.cellProfileId = cellProfileId;
        this.cellPanelID = cellPanelID;
    }

    /**
     * Constructor.
     *
     * @param dataFile         Data File containing Copy Number Alteration, MRNA Expression Data, or protein RPPA data
     * @param cellProfileId CellProfile ID.
     */
    public ImportTabCellData(File dataFile, int cellProfileId, String cellPanelID) {
        this.dataFile = dataFile;
        this.cellProfileId = cellProfileId;
        this.cellPanelID = cellPanelID;
    }

    /**
     * Import the Copy Number Alteration, mRNA Expression, protein RPPA or GSVA data
     *
     * @throws IOException  IO Error.
     * @throws DaoException Database Error.
     */
    public void importData(int numLines) throws IOException, DaoException {

        cellProfile = DaoCellProfile.getCellProfileById(cellProfileId);

        FileReader reader = new FileReader(dataFile);
        BufferedReader buf = new BufferedReader(reader);
        String headerLine = buf.readLine();
        String parts[] = headerLine.split("\t");
        
        boolean continuousCellProfile = cellProfile!=null
                                        && cellProfile.getCellAlterationType() == CellAlterationType.CELL_RELATIVE_ABUNDANCE;
        int numRecordsToAdd = 0;
        int samplesSkipped = 0;
        try {
            int uniqueNameIndex = getUniqueNameIndex(parts); // index of unique_name
            int uniqueIdIndex = getUniqueIdIndex(parts); // index of unique_id
            int cellsetIdIndex = getCellsetIdIndex(parts);   // index of unique_set, not implemented by now
            int sampleStartIndex = getStartIndex(parts, uniqueNameIndex, uniqueIdIndex, cellsetIdIndex);
            if (uniqueNameIndex == -1 && uniqueIdIndex == -1) {
                throw new RuntimeException("Error: at least one of the following columns should be present: UNIQUE_CELL_NAME or UNIQUE_CELL_ID");
            }
            
            String sampleIds[];
            sampleIds = new String[parts.length - sampleStartIndex];
            System.arraycopy(parts, sampleStartIndex, sampleIds, 0, parts.length - sampleStartIndex);

            int nrUnknownSamplesAdded = 0;
            ProgressMonitor.setCurrentMessage(" --> total number of samples: " + sampleIds.length);            
    
            // link Samples to the cell profile
            ArrayList <Integer> orderedSampleList = new ArrayList<Integer>();
            ArrayList <Integer> filteredSampleIndices = new ArrayList<Integer>();

            for (int i = 0; i < sampleIds.length; i++) {
                /* NOTE: disabled add the sample on the fly process, sample shall always exist
                // backwards compatible part (i.e. in the new process, the sample should already be there. TODO - replace this workaround later with an exception:
                Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cellProfile.getCancerStudyId(),
                                                                           StableIdUtil.getSampleId(sampleIds[i]));
                
                if (sample == null ) {
                    //TODO - as stated above, this part should be removed. Agreed with JJ to remove this as soon as MSK moves to new validation 
                    //procedure. In this new procedure, Patients and Samples should only be added 
                    //via the corresponding ImportClinicalData process. Furthermore, the code below is wrong as it assumes one 
                    //sample per patient, which is not always the case.
                    ImportDataUtil.addPatients(new String[] { sampleIds[i] }, cellProfileId);
                    // add the sample (except if it is a 'normal' sample):
                    nrUnknownSamplesAdded += ImportDataUtil.addSamples(new String[] { sampleIds[i] }, cellProfileId);
                }
                // check again (repeated because of workaround above):
                */
                Sample sample = DaoSample.getSampleByCancerStudyAndSampleId(cellProfile.getCancerStudyId(),
                                                                           StableIdUtil.getSampleId(sampleIds[i]));
                // can be null in case of 'normal' sample:
                if (sample == null) {
                    if (StableIdUtil.isNormal(sampleIds[i])) {
                        filteredSampleIndices.add(i);
                        samplesSkipped++;
                        continue;
                    }
                    else {
                        throw new RuntimeException("Unknown sample id '" + StableIdUtil.getSampleId(sampleIds[i]) + "' found in tab-delimited file: " + this.dataFile.getCanonicalPath());
                    }
                }
                ImportDataUtil.addSampleProfile(sample, cellProfileId, cellPanelID);
                orderedSampleList.add(sample.getInternalId());
            }
            
            if (nrUnknownSamplesAdded > 0) {
                ProgressMonitor.logWarning("WARNING: Number of samples added on the fly because they were missing in clinical data:  " + nrUnknownSamplesAdded);
            }
            if (samplesSkipped > 0) {
                ProgressMonitor.setCurrentMessage(" --> total number of samples skipped (normal samples): " + samplesSkipped);
            }
            ProgressMonitor.setCurrentMessage(" --> total number of data lines:  " + (numLines-1));
            
            // add samples to the IM_cell_profile_samples table
            DaoCellProfileSamples.addCellProfileSamples(cellProfileId, orderedSampleList);
    
            // Cell cache:
            DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
    
            // Object to insert records in the 'IM_cell_alteration' table: 
            DaoCellAlteration daoCellAlteration = DaoCellAlteration.getInstance();
    
            //cache for data found in  cna_event' table:
            /* disabled CNA stuff
            Map<CnaEvent.Event, CnaEvent.Event> existingCnaEvents = null;            
            if (discretizedCnaProfile) {
                existingCnaEvents = new HashMap<CnaEvent.Event, CnaEvent.Event>();
                for (CnaEvent.Event event : DaoCnaEvent.getAllCnaEvents()) {
                    existingCnaEvents.put(event, event);
                }
                MySQLbulkLoader.bulkLoadOn();
            }                
            */
            
            int lenParts = parts.length;
            
            String line = buf.readLine();
            while (line != null) {
                ProgressMonitor.incrementCurValue();
                ConsoleUtil.showProgress();
                boolean recordAdded = false;
                
                // either parse line as cellset or cell for importing into 'cell_alteration' table
                /* NOTE: so many types, narrow down to just one type cell, cout matrix
                if (gsvaProfile) {
                    recordAdded = parseCellsetLine(line, lenParts, sampleStartIndex, cellsetIdIndex, 
                            filteredSampleIndices, daoCellAlteration);
                }
                else {
                    recordAdded = parseLine(line, lenParts, sampleStartIndex, 
                            uniqueNameIndex, uniqueIdIndex, rppaCellRefIndex, 
                            rppaProfile, discretizedCnaProfile,
                            daoCell, 
                            filteredSampleIndices, orderedSampleList, 
                            existingCnaEvents, daoCellAlteration);
                }
                */
                
                recordAdded = parseLine(line, lenParts, sampleStartIndex,
                    uniqueNameIndex, uniqueIdIndex, -1,
                    false, false,
                    daoCell,
                    filteredSampleIndices, orderedSampleList,
                    null, daoCellAlteration);
                
                // increment number of records added or entries skipped
                if (recordAdded) {
                    numRecordsToAdd++;
                }
                else {
                    entriesSkipped++;
                }
                
                line = buf.readLine();
            }
            if (MySQLbulkLoader.isBulkLoad()) {
               MySQLbulkLoader.flushAll();
            }
            
            if (entriesSkipped > 0) {
                ProgressMonitor.setCurrentMessage(" --> total number of data entries skipped (see table below):  " + entriesSkipped);
            }

            if (numRecordsToAdd == 0) {
                throw new DaoException ("Something has gone wrong!  I did not save any records" +
                        " to the database!");
            }
        }
        finally {
            buf.close();
        }                
    }
    
    private boolean parseLine(String line, int nrColumns, int sampleStartIndex, 
            int uniqueNameIndex, int uniqueIdIndex, int rppaCellRefIndex,
            boolean rppaProfile, boolean discretizedCnaProfile,
            DaoCellOptimized daoCell,
            List <Integer> filteredSampleIndices, List <Integer> orderedSampleList,
            Map<CnaEvent.Event, CnaEvent.Event> existingCnaEvents, DaoCellAlteration daoCellAlteration
            ) throws DaoException {
        
        boolean recordStored = false; 
        
        //  Ignore lines starting with #
        if (!line.startsWith("#") && line.trim().length() > 0) {
            String[] parts = line.split("\t",-1);
            
            if (parts.length>nrColumns) {
                if (line.split("\t").length>nrColumns) {
                    ProgressMonitor.logWarning("Ignoring line with more fields (" + parts.length
                            + ") than specified in the headers(" + nrColumns + "): \n"+parts[0]);
                    return false;
                }
            }
            String values[] = (String[]) ArrayUtils.subarray(parts, sampleStartIndex, parts.length>nrColumns?nrColumns:parts.length);
            values = filterOutNormalValues(filteredSampleIndices, values);

            String cellSymbol = null;
            if (uniqueNameIndex != -1) {
                cellSymbol = parts[uniqueNameIndex];
            }
            /* NOTE disabled RPPA Stuff 
            //RPPA: //TODO - we should split up the RPPA scenario from this code...too many if/else because of this
            if (rppaCellRefIndex != -1) {
                cellSymbol = parts[rppaCellRefIndex];
            }
            if (cellSymbol!=null && cellSymbol.isEmpty()) {
                cellSymbol = null;
            }
            if (rppaProfile && cellSymbol == null) {
                ProgressMonitor.logWarning("Ignoring line with no Composite.Element.REF value");
                return false;
            }*/
            //get uniqueId
            String uniqueId = null;
            if (uniqueIdIndex!=-1) {
                uniqueId = parts[uniqueIdIndex];
            }
            if (uniqueId!=null) {
                if (uniqueId.isEmpty()) {
                    uniqueId = null;
                }
                else if (!uniqueId.matches("[0-9]+")) {
                    //TODO - would be better to give an exception in some cases, like negative Unique values
                    ProgressMonitor.logWarning("Ignoring line with invalid UNIQUE_CELL_ID " + uniqueId);
                    return false;
                }                
            }
            
            //If all are empty, skip line:
            if (cellSymbol == null && uniqueId == null) {
                ProgressMonitor.logWarning("Ignoring line with no CELL_UNIQUE_NAME or CELL_UNIQUE_ID value");
                return false;
            }
            else {
                if (cellSymbol != null && (cellSymbol.contains("///") || cellSymbol.contains("---"))) {
                    //  Ignore cell IDs separated by ///.  This indicates that
                    //  the line contains information regarding multiple cells, and
                    //  we cannot currently handle this.
                    //  Also, ignore cell IDs that are specified as ---.  This indicates
                    //  the line contains information regarding an unknown cell, and
                    //  we cannot currently handle this.
                    ProgressMonitor.logWarning("Ignoring cell ID:  " + cellSymbol);
                    return false;
                } else {
                    List<CanonicalCell> cells = null;
                    
                    /*NOTE: disabled RPPA stuff
                    //If rppa, parse cells from "Composite.Element.REF" column:
                    if (rppaProfile) {
                        cells = parseRPPACells(cellSymbol);
                        if (cells == null) {
                            //will be null when there is a parse error in this case, so we
                            //can return here and avoid duplicated messages:
                            return false;
                        }    
                    }
                    else {
                    */
                    
                    //try uniqueId:
                    if (uniqueId != null) {
                        CanonicalCell cell = daoCell.getCell(Long.parseLong(uniqueId));
                        if (cell != null) {
                            cells = Arrays.asList(cell);
                        }
                    }
                    //no uniqueId or could not resolve by uniqueId, try hugo:
                    if ((cells == null || cells.isEmpty()) && cellSymbol != null) {
                        // deal with multiple symbols separate by |, use the first one
                        int ix = cellSymbol.indexOf("|");
                        if (ix>0) {
                            cellSymbol = cellSymbol.substring(0, ix);
                        }
                        cells = daoCell.getCell(cellSymbol, true);
                    }
                    //if cells still null, skip current record
                    if (cells == null || cells.isEmpty()) {
                        ProgressMonitor.logWarning("UNIQUE_CELL_ID " + uniqueId + " not found. Record will be skipped for this cell.");
                        return false;
                    }
                    
                    if (cells == null || cells.isEmpty()) {
                        cells = Collections.emptyList();
                    }

                    //  If no target line is specified or we match the target, process.
                    if (targetLine == null || parts[0].equals(targetLine)) {
                        if (cells.isEmpty()) {
                            //  if cell is null, we might be dealing with a micro RNA ID
                            if (cellSymbol != null && cellSymbol.toLowerCase().contains("-mir-")) {
//                                if (microRnaIdSet.contains(cellId)) {
//                                    storeMicroRnaAlterations(values, daoMicroRnaAlteration, cellId);
//                                    numRecordsStored++;
//                                } else {
                                    ProgressMonitor.logWarning("microRNA is not known to me:  [" + cellSymbol
                                        + "]. Ignoring it "
                                        + "and all tab-delimited data associated with it!");
                                    return false;
//                                }
                            } else {
                                String cell = (cellSymbol != null) ? cellSymbol : uniqueId;
                                ProgressMonitor.logWarning("Cell not found for:  [" + cell
                                    + "]. Ignoring it "
                                    + "and all tab-delimited data associated with it!");
                                return false;
                            }
                        } else if (cells.size()==1) {
                            /* NOTE: disabled CNA stuff
                            List<CnaEvent> cnaEventsToAdd = new ArrayList<CnaEvent>();
                            
                            if (discretizedCnaProfile) {
                                long uniqueId = cells.get(0).getUniqueId();
                                for (int i = 0; i < values.length; i++) {
                                    
                                    // temporary solution -- change partial deletion back to full deletion.
                                    if (values[i].equals(CNA_VALUE_PARTIAL_DELETION)) {
                                        values[i] = CNA_VALUE_HOMOZYGOUS_DELETION;
                                    }
                                    if (values[i].equals(CNA_VALUE_AMPLIFICATION) 
                                           // || values[i].equals(CNA_VALUE_GAIN)  >> skipping GAIN, ZERO, HEMIZYGOUS_DELETION to minimize size of dataset in DB
                                           // || values[i].equals(CNA_VALUE_ZERO)
                                           // || values[i].equals(CNA_VALUE_HEMIZYGOUS_DELETION)
                                            || values[i].equals(CNA_VALUE_HOMOZYGOUS_DELETION)) {
                                        CnaEvent cnaEvent = new CnaEvent(orderedSampleList.get(i), cellProfileId, uniqueId, Short.parseShort(values[i]));
                                        //delayed add:
                                        cnaEventsToAdd.add(cnaEvent);
                                    }
                                }
                            }
                            */
                            recordStored = storeCellAlterations(values, daoCellAlteration, cells.get(0), cellSymbol);
                            /* NOTE: disabled CNA stuff
                            //only add extra CNA related records if the step above worked, otherwise skip:
                            if (recordStored) {
                                for (CnaEvent cnaEvent : cnaEventsToAdd) {
                                    if (existingCnaEvents.containsKey(cnaEvent.getEvent())) {
                                        cnaEvent.setEventId(existingCnaEvents.get(cnaEvent.getEvent()).getEventId());
                                        DaoCnaEvent.addCaseCnaEvent(cnaEvent, false);
                                    } else {
                                        //cnaEvent.setEventId(++cnaEventId); not needed anymore, column now has AUTO_INCREMENT 
                                        DaoCnaEvent.addCaseCnaEvent(cnaEvent, true);
                                        existingCnaEvents.put(cnaEvent.getEvent(), cnaEvent.getEvent());
                                    }
                                }
                            }                            
                            */
                        } 
                        /* NOTE: disabled various stuff
                        else {
                            int otherCase = 0;
                            for (CanonicalCell cell : cells) {
                                if (cell.isMicroRNA() || rppaProfile) { // for micro rna or protein data, duplicate the data
                                    boolean result = storeCellAlterations(values, daoCellAlteration, cell, cellSymbol);
                                    if (result == true) {
                                        recordStored = true;
                                        nrExtraRecords++;
                                    }
                                }
                                else {
                                    otherCase++;
                                }
                            }
                            if (recordStored) {
                                //skip one, to avoid double counting:
                                nrExtraRecords--;
                            }
                            if (!recordStored) {
                                if (otherCase == 0) {
                                    // this means that miRNA or RPPA could not be stored
                                    ProgressMonitor.logWarning("Could not store miRNA or RPPA data"); //TODO detect the type of of data and give specific warning
                                }
                                else if (otherCase > 1) {
                                    // this means that cells.size() > 1 and data was not rppa or microRNA, so it is not defined how to deal with
                                    // the ambiguous alias list. Report this:
                                    ProgressMonitor.logWarning("Cell symbol " + cellSymbol + " found to be ambigous. Record will be skipped for this cell.");
                                }
                                else {
                                    //should not occur. It would mean something is wrong in preceding logic (see else if (cells.size()==1) ) or a configuration problem, e.g. where a symbol maps to both a miRNA and a normal cell:
                                    throw new RuntimeException("Unexpected error: unable to process row with cell " + cellSymbol);
                                }
                            }
                        }
                        */
                    }
                }
            }
        }
        return recordStored;
    }
    
    /**
     * Parses line for cell set record and stores record in 'cell_alteration' table.
     * @param line
     * @param nrColumns
     * @param sampleStartIndex
     * @param cellsetIdIndex
     * @param filteredSampleIndices
     * @param daoCellAlteration
     * @return
     * @throws DaoException 
     */
    /* NOTE: disabled cellset stuff
    private boolean parseCellsetLine(String line, int nrColumns, int sampleStartIndex, int cellsetIdIndex,
             List<Integer> filteredSampleIndices, DaoCellAlteration daoCellAlteration) throws DaoException {
        boolean storedRecord = false;
        
        if (!line.startsWith("#") && line.trim().length() > 0) {
            String[] parts = line.split("\t",-1);

            if (parts.length>nrColumns) {
                if (line.split("\t").length>nrColumns) {
                    ProgressMonitor.logWarning("Ignoring line with more fields (" + parts.length
                                        + ") than specified in the headers(" + nrColumns + "): \n"+parts[0]);
                    return false;
                }
            }
            
            String values[] = (String[]) ArrayUtils.subarray(parts, sampleStartIndex, parts.length>nrColumns?nrColumns:parts.length);
            values = filterOutNormalValues(filteredSampleIndices, values);
            
            Cellset cellset = DaoCellset.getCellsetByExternalId(parts[cellsetIdIndex]);
            if (cellset !=  null) {
                storedRecord = storeCellEntityCellAlterations(values, daoCellAlteration, cellset.getCellEntityId(), 
                        DaoCellEntity.EntityTypes.GENESET, cellset.getExternalId());
            }
            else {
                ProgressMonitor.logWarning("Cellset " + parts[cellsetIdIndex] + " not found in DB. Record will be skipped.");
            }
        }
        return storedRecord;
    }
    */

    private boolean storeCellAlterations(String[] values, DaoCellAlteration daoCellAlteration,
            CanonicalCell cell, String cellSymbol) throws DaoException {
        //  Check that we have not already imported information regarding this cell.
        //  This is an important check, because a GISTIC or RAE file may contain
        //  multiple rows for the same cell, and we only want to import the first row.
        try {
            if (!importSetOfCells.contains(cell.getUniqueCellId())) {
                daoCellAlteration.addCellAlterations(cellProfileId, cell.getUniqueCellId(), values);
                importSetOfCells.add(cell.getUniqueCellId());
                return true;
            }
            else {
                //TODO - review this part - maybe it should be an Exception instead of just a warning.
                String cellSymbolMessage = "";
                if (cellSymbol != null && !cellSymbol.equalsIgnoreCase(cell.getUniqueCellNameAllCaps()))
                    cellSymbolMessage = "(given as alias in your file as: " + cellSymbol + ") ";
                ProgressMonitor.logWarning("Cell " + cell.getUniqueCellNameAllCaps() + " (" + cell.getUniqueCellId() + ")" + cellSymbolMessage + " found to be duplicated in your file. Duplicated row will be ignored!");
                return false;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Aborted: Error found for row starting with " + cellSymbol + ": " + e.getMessage());
        }
    }
    
    /**
     * Stores cell alteration data for a cell entity. 
     * @param values
     * @param daoCellAlteration
     * @param cellEntityId - internal id for cell entity
     * @param cellEntityType - "GENE", "GENESET", "PHOSPHOPROTEIN"
     * @param cellEntityName - hugo symbol for "GENE", external id for "GENESET", phospho cell name for "PHOSPHOPROTEIN"
     * @return boolean indicating if record was stored successfully or not
     */
    /* NOTE: disabled cell set stuff
    private boolean storeCellEntityCellAlterations(String[] values, DaoCellAlteration daoCellAlteration,
        Integer cellEntityId, DaoCellEntity.EntityTypes cellEntityType, String cellEntityName) {
        try {
            if (importedCellEntitySet.add(cellEntityId)) {
                daoCellAlteration.addCellAlterationsForCellEntity(cellProfile.getCellProfileId(), cellEntityId, values);
                return true;
            }
            else {
                ProgressMonitor.logWarning("Data for cell entity " + cellEntityName 
                    + " [" + cellEntityType +"] already imported from file. Record will be skipped.");
                return false;
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Aborted: Error found for row starting with " + cellEntityName + ": " + ex.getMessage());
        }
    }
    */

    /**
     * Tries to parse the cells and look them up in DaoCellOptimized
     * 
     * @param antibodyWithCell
     * @return returns null if something was wrong, e.g. could not parse the antibodyWithCell string; returns 
     * a list with 0 or more elements otherwise.
     * @throws DaoException
     */
    /* NOTE: disabled RPPA stuff
    private List<CanonicalCell> parseRPPACells(String antibodyWithCell) throws DaoException {
        DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
        String[] parts = antibodyWithCell.split("\\|");
        //validate:
        if (parts.length < 2) {
            ProgressMonitor.logWarning("Could not parse Composite.Element.Ref value " + antibodyWithCell + ". Record will be skipped.");
            //return null when there was a parse error:
            return null;
        }
        String[] symbols = parts[0].split(" ");
        String arrayId = parts[1];
        //validate arrayId: if arrayId if duplicated, warn:
        if (!arrayIdSet.add(arrayId)) {
            ProgressMonitor.logWarning("Id " + arrayId + " in [" + antibodyWithCell + "] found to be duplicated. Record will be skipped.");
            return null;
        }
        List<String> symbolsNotFound = new ArrayList<String>();
        List<CanonicalCell> cells = new ArrayList<CanonicalCell>();
        for (String symbol : symbols) {
            if (symbol.equalsIgnoreCase("NA")) {
                //workaround because of bug in firehose. See https://github.com/cBioPortal/cbioportal/issues/839#issuecomment-203523078
                ProgressMonitor.logWarning("Cell " + symbol + " will be interpreted as 'Not Available' in this case. Record will be skipped for this cell.");
            }
            else {
                CanonicalCell cell = daoCell.getNonAmbiguousCell(symbol, null);
                if (cell!=null) {
                    cells.add(cell);
                }
                else {
                    symbolsNotFound.add(symbol);
                }
            }
        }
        if (cells.size() == 0) {
            //return empty list:
            return cells;
        }
        //So one or more cells were found, but maybe some were not found. If any 
        //is not found, report it here:
        for (String symbol : symbolsNotFound) {
            ProgressMonitor.logWarning("Cell " + symbol + " not found in DB. Record will be skipped for this cell.");
        }
        
        Pattern p = Pattern.compile("(p[STY][0-9]+(?:_[STY][0-9]+)*)");
        Matcher m = p.matcher(arrayId);
        String residue;
        if (!m.find()) {
            //type is "protein_level":
            return cells;
        } else {
            //type is "phosphorylation":
            residue = m.group(1);
            return importPhosphoCell(cells, residue);
        }
    }
    
    private List<CanonicalCell> importPhosphoCell(List<CanonicalCell> cells, String residue) throws DaoException {
        DaoCellOptimized daoCell = DaoCellOptimized.getInstance();
        List<CanonicalCell> phosphoCells = new ArrayList<CanonicalCell>();
        for (CanonicalCell cell : cells) {
            Set<String> aliases = new HashSet<String>();
            aliases.add("rppa-phospho");
            aliases.add("phosphoprotein");
            aliases.add("phospho"+cell.getStandardSymbol());
            String phosphoSymbol = cell.getStandardSymbol()+"_"+residue;
            CanonicalCell phosphoCell = daoCell.getCell(phosphoSymbol);
            if (phosphoCell==null) {
                ProgressMonitor.logWarning("Phosphoprotein " + phosphoSymbol + " not yet known in DB. Adding it to `cell` table with 3 aliases in `cell_alias` table.");
                phosphoCell = new CanonicalCell(phosphoSymbol, aliases);
                phosphoCell.setType(CanonicalCell.PHOSPHOPROTEIN_TYPE);
                phosphoCell.setCytoband(cell.getCytoband());
                daoCell.addCell(phosphoCell);
            }
            phosphoCells.add(phosphoCell);
        }
        return phosphoCells;
    }
    */
    
    // returns index for cellset id column
    private int getCellsetIdIndex(String[] headers) {
        for (int i=0; i<headers.length; i++) {
            if (headers[i].equalsIgnoreCase("UNIQUE_CELLSET_ID" )) {
                return i;
            }
        }
        return -1;
    }
    
    private int getUniqueNameIndex(String[] headers) {
        for (int i = 0; i<headers.length; i++) {
            if (headers[i].equalsIgnoreCase("UNIQUE_CELL_NAME")) {
                return i;
            }
        }
        return -1;
    }
    
    private int getUniqueIdIndex(String[] headers) {
        for (int i = 0; i<headers.length; i++) {
            if (headers[i].equalsIgnoreCase("UNIQUE_CELL_ID")) {
                return i;
            }
        }
        return -1;
    }

    /* NOTE: disabled RPPA stuff
    private int getRppaCellRefIndex(String[] headers) {
        for (int i = 0; i<headers.length; i++) {
            if (headers[i].equalsIgnoreCase("Composite.Element.Ref")) {
                return i;
            }
        }
        return -1;
    }
    */
    
    private int getStartIndex(String[] headers, int uniqueNameIndex, int uniqueIdIndex, int cellsetIdIndex) {
        int startIndex = -1;
        
        for (int i=0; i<headers.length; i++) {
            String h = headers[i];
            //if the column is not one of the cell symbol/cell ide columns or other pre-sample columns:
            if (!h.equalsIgnoreCase("UNIQUE_CELL_NAME") &&
                    !h.equalsIgnoreCase("UNIQUE_CELL_ID") &&
                    !h.equalsIgnoreCase("UNIQUE_CELLSET_ID")) {
                //and the column is found after  uniqueNameIndex and uniqueIdIndex: 
                if (i > uniqueNameIndex && i > uniqueIdIndex && i > cellsetIdIndex) {
                    //then we consider this the start of the sample columns:
                    startIndex = i;
                    break;
                }
            }
        }
        if (startIndex == -1)
            throw new RuntimeException("Could not find a sample column in the file");
        
        return startIndex;
    }

    private String[] filterOutNormalValues(List <Integer> filteredSampleIndices, String[] values)
    {
        ArrayList<String> filteredValues = new ArrayList<String>();
        for (int lc = 0; lc < values.length; lc++) {
            if (!filteredSampleIndices.contains(lc)) {
                filteredValues.add(values[lc]);
            }
        }
        return filteredValues.toArray(new String[filteredValues.size()]);
    }
}
