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

package org.mskcc.cbio.portal.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.mskcc.cbio.portal.model.CanonicalCell;
import org.mskcc.cbio.portal.util.ProgressMonitor;

/**
 * A Utility Class that speeds access to Cell Info.
 *
 * @author Ethan Cerami
 */
public class DaoCellOptimized {
    
    /* NOTE: make sure these filenames are synced with core/src/main/resources filenames */
    private static final String IM_CANCER_CELLS_FILE = "/im_cancer_cells.txt";
    private static final String CELL_SYMBOL_DISAMBIGUATION_FILE = "/cell_symbol_disambiguation.txt";
        
    private static final DaoCellOptimized daoCellOptimized = new DaoCellOptimized();
    //nb: make sure any map is also cleared in clearCache() method below:
    private final HashMap<String, CanonicalCell> cellSymbolMap = new HashMap <String, CanonicalCell>();
    private final HashMap<Long, CanonicalCell> uniqueCellIdMap = new HashMap <Long, CanonicalCell>();
    private final HashMap<Integer, CanonicalCell> cellEntityMap = new HashMap<Integer, CanonicalCell>();
    private final HashMap<String, List<CanonicalCell>> cellAliasMap = new HashMap<String, List<CanonicalCell>>();
    private final Set<CanonicalCell> imCancerCells = new HashSet<CanonicalCell>();
    private final Map<String, CanonicalCell> disambiguousCells = new HashMap<String, CanonicalCell>();
    
    /**
     * Private Constructor, to enforce singleton pattern.
     * 
     * @throws DaoException Database Error.
     */
    private DaoCellOptimized () {
        fillCache();
    }
    
    private synchronized void fillCache() {
        try {
            //  Automatically populate hashmap upon init
            ArrayList<CanonicalCell> globalCellList = DaoCell.getAllCells();
            for (CanonicalCell currentCell:  globalCellList) {
                cacheCell(currentCell);
            }
        } catch (DaoException e) {
            e.printStackTrace();
        }
        
        System.err.println(cellSymbolMap.size());
        
        try {
            
            System.err.println("CANCER_CELLS");
            
            if (cellSymbolMap.size()>10000) { 
                // only for deployed version; not for unit test and importing
                BufferedReader in = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream(IM_CANCER_CELLS_FILE)));
                for (String line=in.readLine(); line!=null; line=in.readLine()) {
                    String[] parts = line.trim().split("\t",-1);
                    CanonicalCell cell = null;
                    if (parts.length>1) {
                        cell = getCell(Long.parseLong(parts[1]));
                    } else {
                        cell = getCell(parts[0]);
                    }
                    if (cell!=null) {
                        imCancerCells.add(cell);
                    } else {
                    	ProgressMonitor.logWarning(line+" in the immube cancer cell list config file [resources" + IM_CANCER_CELLS_FILE + 
                        		"] is not a unique cell name. You should either update this file or update the `IM_cell` and `IM_cell_alias` tables to fix this.");
                    }
                }
                in.close();
            }

            System.err.println("CELL_SYMBOL");
            
            {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(getClass().getResourceAsStream(CELL_SYMBOL_DISAMBIGUATION_FILE)));
                for (String line=in.readLine(); line!=null; line=in.readLine()) {
                    if (line.startsWith("#")) {
                        continue;
                    }
                    String[] parts = line.trim().split("\t",-1);
                    CanonicalCell cell = getCell(Long.parseLong(parts[1]));
                    if (cell==null) {
                    	ProgressMonitor.logWarning(line+" in config file [resources" + CELL_SYMBOL_DISAMBIGUATION_FILE + 
                        		"]is not valid. You should either update this file or update the `IM_cell` and `IM_cell_alias` tables to fix this.");
                    }
                    disambiguousCells.put(parts[0], cell);
                }
                in.close();
            }
            
            System.err.println("OTHER PROBLEM");
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void clearCache()
    {
        cellSymbolMap.clear();
        uniqueCellIdMap.clear();
        cellEntityMap.clear();
        cellAliasMap.clear();
        imCancerCells.clear();
        disambiguousCells.clear();
    }

    /**
     * Clear and fill cache again. Useful for unit tests and 
     * for the Import procedure to update the cells table, clearing the
     * cache without the need to restart the webserver.
     */
    public synchronized void reCache()
    {
        clearCache();
        fillCache();
    }
   
    /**
     * Adds a new Cell Record to the Database. If the Unique Cell ID is negative,
     * a fake Unique Cell ID will be assigned.
     * @param cell  Canonical Cell Object.
     * @return number of records successfully added.
     * @throws DaoException Database Error.
     */
    public int addCell(CanonicalCell cell) throws DaoException {
        int ret;
        if (cell.getUniqueCellId()>0) {
            ret = DaoCell.addOrUpdateCell(cell); //This is a valid UniqueCellId, add as is
        } else {
            ret = DaoCell.addCellWithoutUniqueCellId(cell); //Generate a unique negative fake id
        }
        //only overwrite cache if ..?
        cacheCell(cell);
        return ret;
    }
    
    /**
     * Update Cell Record in the Database. It will also replace this 
     * cell's aliases with the ones found in the given cell object.
     */
    public int updateCell(CanonicalCell cell)  throws DaoException {
        int ret = DaoCell.updateCell(cell);
        //recache:
        cacheCell(cell);
        return ret;
    }
    
    public void deleteCell(CanonicalCell cell) throws DaoException {
        DaoCell.deleteCell(cell.getUniqueCellId());
        cellSymbolMap.remove(cell.getUniqueCellNameAllCaps());
        for (String alias : cell.getAliases()) {
            String aliasUp = alias.toUpperCase();
            List<CanonicalCell> cells = cellAliasMap.get(aliasUp);
            cells.remove(cell);
            if (cells.isEmpty()) {
                cellAliasMap.remove(aliasUp);
            }
        }
    }
    
    private void cacheCell(CanonicalCell cell) {
        cellSymbolMap.put(cell.getUniqueCellNameAllCaps(), cell);
        uniqueCellIdMap.put(cell.getUniqueCellId(), cell);
        cellEntityMap.put(cell.getCellEntityId(), cell);

        for (String alias : cell.getAliases()) {
            String aliasUp = alias.toUpperCase();
            List<CanonicalCell> cells = cellAliasMap.get(aliasUp);
            if (cells==null) {
                cells = new ArrayList<CanonicalCell>();
                cellAliasMap.put(aliasUp, cells);
            }
            cells.add(cell);
        }
    }

    /**
     * Gets Global Singleton Instance.
     *
     * @return DaoCellOptimized Singleton.
     * @throws DaoException Database Error.
     */
    public static DaoCellOptimized getInstance() {
        return daoCellOptimized;
    }
    
    /**
     * Return cellEntityId from cache for given uniqueCellId
     * 
     * @param uniqueCellId
     * @return
     */
    public static int getCellEntityId(long uniqueCellId) {
    	//get entity id from cache:
    	CanonicalCell cell = daoCellOptimized.getCell(uniqueCellId);
		if (cell != null) {
			return cell.getCellEntityId();
		}
		else {
			throw new RuntimeException("Invalid uniqueCellId symbol. Not found in cache: " + uniqueCellId);
		}    			
    }
    
    /**
     * Return uniqueCellId from cache for given cellEntityId
     * 
     * @param cellEntityId
     * @return
     */
    public static long getUniqueCellId(int cellEntityId) {
    	//get entity id from cache:
    	CanonicalCell cell = daoCellOptimized.getCellByEntityId(cellEntityId);
		//since not every cell entity will be a cell, this could be null (but would
    	//be a programming error elsewhere, so throw exception):
    	if (cell == null) {
    		throw new RuntimeException("Cell entity was not found in cell cache: " + cellEntityId);
    	}
    		
    	return daoCellOptimized.getCellByEntityId(cellEntityId).getUniqueCellId();
    }

    /**
     * Gets Cell by Unique Cell Symbol.
     *
     * @param uniqueCellName Unique Cell Symbol.
     * @return Canonical Cell Object.
     */
    public CanonicalCell getCell(String uniqueCellName) {
        return cellSymbolMap.get(uniqueCellName.toUpperCase());
    }

    /**
     * Looks for a Cell where Unique Cell Symbol or an alias matches the given symbol. 
     * 
     * @param cellSymbol: Unique Cell Symbol or an alias
     * @param searchInAliases: set to true if this method should search for a match in this.cellAliasMap 
     * in case a matching cell symbol cannot be found in this.cellSymbolMap
     * 
     * @return
     */
    public List<CanonicalCell> getCell(String cellSymbol, boolean searchInAliases) {
    	CanonicalCell cell = getCell(cellSymbol);
    	if (cell!=null) {
            return Collections.singletonList(cell);
        }
        
    	if (searchInAliases) {
	        List<CanonicalCell> cells = cellAliasMap.get(cellSymbol.toUpperCase());
	        if (cells!=null) {
	        	return Collections.unmodifiableList(cells);
	        }
        }
        
        return Collections.emptyList();
    }
    
    /**
     * Gets Cell By Unique Cell ID.
     *
     * @param uniqueCellId Unique Cell ID.
     * @return Canonical Cell Object.
     */
    public CanonicalCell getCell(long uniqueCellId) {
        return uniqueCellIdMap.get(uniqueCellId);
    }
    
    /**
     * Gets Cell By Unique Cell ID.
     *
     * @param cellEntityId Cell Entity ID.
     * @return Canonical Cell Object.
     */
    public CanonicalCell getCellByEntityId(int cellEntityId) {
        return cellEntityMap.get(cellEntityId);
    }
    
    /**
     * Look for cells with a specific ID. First look for cells with the specific
     * Unique Cell ID, if found return this cell; then for Cell symbol, if found,
     * return this cell; and lastly for aliases, if found, return a list of
     * matched cells (could be more than one). If nothing matches, return an 
     * empty list.
     * @param cellId an Unique Cell ID or Cell symbol or cell alias
     * @return A list of cells that match, an empty list if no match.
     */
    public List<CanonicalCell> guessCell(String cellId) {
        return guessCell(cellId, null);
    }
    
    /**
     * Look for cells with a specific ID on an organ. First look for cells with the specific
     * Unique Cell ID, if found return this cell; then for Cell symbol, if found,
     * return this cell; and lastly for aliases, if found, return a list of
     * matched cells (could be more than one). If organ is not null, use that to match too.
     * If nothing matches, return an empty list.
     * @param cellId an Unique Cell ID or Cell symbol or cell alias
     * @param organ
     * @return A list of cells that match, an empty list if no match.
     */
    public List<CanonicalCell> guessCell(String cellId, String organ) {
        if (cellId==null) {
            return Collections.emptyList();
        }
        
        CanonicalCell cell;
        if (cellId.matches("[0-9]+")) { // likely to be a unique cell id (int)
            cell = getCell(Integer.parseInt(cellId));
            if (cell!=null) {
                return Collections.singletonList(cell);
            }
        }
        
        cell = getCell(cellId); // cell name
        if (cell!=null) {
            return Collections.singletonList(cell);
        }
        
        List<CanonicalCell> cells = cellAliasMap.get(cellId.toUpperCase());
        if (cells!=null) {
            if (organ==null) {
                return Collections.unmodifiableList(cells);
            }
            
            List<CanonicalCell> ret = new ArrayList<CanonicalCell>();
            for (CanonicalCell cg : cells) {
                String gorgan = cg.getOrgan();
                if (gorgan==null // TODO: should we exlude this?
                    || gorgan.equals(organ)) {
                    ret.add(cg);
                }
            }
            
            return ret;
        }
        
        return Collections.emptyList();
    }
    
    
    /* CODE TO DELETE
    private static Map<String,String> validChrValues = null;
    public static String normalizeChr(String strChr) {
        if (strChr==null) {
            return null;
        }
        
        if (validChrValues==null) {
            validChrValues = new HashMap<String,String>();
            for (int lc = 1; lc<=24; lc++) {
                    validChrValues.put(Integer.toString(lc),Integer.toString(lc));
                    validChrValues.put("CHR" + Integer.toString(lc),Integer.toString(lc));
            }
            validChrValues.put("X","23");
            validChrValues.put("CHRX","23");
            validChrValues.put("Y","24");
            validChrValues.put("CHRY","24");
            validChrValues.put("NA","NA");
            validChrValues.put("MT","MT"); // mitochondria
        }

        return validChrValues.get(strChr);
    }
    
    private static String getChrFromCytoband(String cytoband) {
        if (cytoband==null) {
            return null;
        }
        
        if (cytoband.startsWith("X")) {
            return "23";
        }
        
        if (cytoband.startsWith("Y")) {
            return "24";
        }
        
        Pattern p = Pattern.compile("([0-9]+).*");
        Matcher m = p.matcher(cytoband);
        if (m.find()) {
            return m.group(1);
        }
        
        return null;
    }
    CODE TO DELETE */
    
    /**
     * Look for cell that can be non-ambiguously determined.
     * @param cellId an Unique Cell ID or Cell symbol or cell alias
     * @return a cell that can be non-ambiguously determined, or null if cannot.
     */
    public CanonicalCell getNonAmbiguousCell(String cellId) {
        return getNonAmbiguousCell(cellId, null);
    }
    
    /**
     * Look for cell that can be non-ambiguously determined.
     * @param cellId an Unique Cell ID or Cell symbol or cell alias
     * @param organ
     * @return a cell that can be non-ambiguously determined, or null if cannot.
     */
    public CanonicalCell getNonAmbiguousCell(String cellId, String organ) {
    	return getNonAmbiguousCell(cellId, organ,true);
    }
    
    /**
     * Look for cell that can be non-ambiguously determined
     * @param cellId an Unique Cell ID or Cell symbol or cell alias
     * @param organ
     * @param issueWarning if true and cell is not ambiguous, 
     * print all the Unique Ids corresponding to the cellId provided
     * @return a cell that can be non-ambiguously determined, or null if cannot.
     */
    public CanonicalCell getNonAmbiguousCell(String cellId, String organ, boolean issueWarning) {
        List<CanonicalCell> cells = guessCell(cellId, organ);
        if (cells.isEmpty()) {
            return null;
        }
        
        if (cells.size()==1) {
            return cells.get(0);
        }
        
        if (disambiguousCells.containsKey(cellId)) {
            return disambiguousCells.get(cellId);
        }
        if (issueWarning) {
	        StringBuilder sb = new StringBuilder("Ambiguous alias ");
	        sb.append(cellId);
	        sb.append(": corresponding unique cell ids of ");
	        for (CanonicalCell cell : cells) {
	            sb.append(cell.getUniqueCellId());
	            sb.append(",");
	        }
	        sb.deleteCharAt(sb.length()-1);
	        
	        ProgressMonitor.logWarning(sb.toString());
        }
        return null;
        
    }
    
    public Set<Long> getUniqueCellIds(Collection<CanonicalCell> cells) {
        Set<Long> uniqueCellIds = new HashSet<Long>();
        for (CanonicalCell cell : cells) {
            uniqueCellIds.add(cell.getUniqueCellId());
        }
        return uniqueCellIds;
    }
    
    public Set<CanonicalCell> getImCancerCells() {
        return Collections.unmodifiableSet(imCancerCells);
    }
    
    public boolean isImCancerCell(CanonicalCell cell) {
        return imCancerCells.contains(cell);
    }

    /**
     * Gets an ArrayList of All Cells.
     * @return Array List of All Cells.
     */
    public ArrayList<CanonicalCell> getAllCells () {
        return new ArrayList<CanonicalCell>(uniqueCellIdMap.values());
    }

    /**
     * Deletes all Cell Records in the Database.
     * @throws DaoException Database Error.
     * 
     * @deprecated  only used by deprecated code, so deprecating this as well.
     */
    public void deleteAllRecords() throws DaoException {
        DaoCell.deleteAllRecords();
    }

}