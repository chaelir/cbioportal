/*
 * Copyright (c) 2019 Stanford University
 *
 * TBD
 *
 */

package org.mskcc.cbio.portal.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class to wrap Cell Entity ID etc.
 */
public class CanonicalCell extends Cell {
    private int cellEntityId;  // unique, auto-increment
    private long uniqueCellId; // unique
    private String uniqueCellName; // unique
    private Set<String> aliases;
    private String type;
    private String organ;
    private int cpId;  //cellpedia cell id
    private int anatomyId; //cellpedia anatomy id 
    private int cellTypeId; //cellpedia cell type id

    /**
     * This constructor can be used when cellEntityId is not yet known, 
     * e.g. in case of a new cell (like when adding new cells in ImportCellData), 
     *
     * @param uniqueCellId
     * @param uniqueCellName
     */
    public CanonicalCell(long uniqueCellId, String uniqueCellName) {
        this(-1, uniqueCellId, uniqueCellName, null);
    }

    /**
     * This constructor can be used when cellEntityId is not yet known, 
     * e.g. in case of a new cell (like when adding new cells in ImportCellData), 
     *
     * @param uniqueCellId
     * @param uniqueCellName
     * @param aliases
     */
    public CanonicalCell(long uniqueCellId, String uniqueCellName, Set<String> aliases) {
        this(-1, uniqueCellId, uniqueCellName, aliases);
    }
    
    public CanonicalCell(int cellEntityId, long uniqueCellId, String uniqueCellName, Set<String> aliases) {
   		this.cellEntityId = cellEntityId;
    	this.uniqueCellId = uniqueCellId;
        this.uniqueCellName = uniqueCellName;
        setAliases(aliases);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getAnatomyId() {
        return anatomyId;
    }

    public void setAnatomyId(int anatomyId) {
        this.anatomyId = anatomyId;
    }
    
    public int getCellTypeId() {
        return cellTypeId;
    }

    public void setCellTypeId(int cellTypeId) {
        this.cellTypeId = cellTypeId;
    }

    public String getOrgan() {
        return organ;
    }

    public int getCpId() {
        return cpId;
    }

    public void setCpId(int cpId) {
        this.cpId = cpId;
    }
    
    public void setOrgan(String organ) {
        this.organ = organ;
    }

    public Set<String> getAliases() {
        if (aliases==null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(aliases);
    }

    public void setAliases(Set<String> aliases) {
        if (aliases==null) {
            this.aliases = null;
            return;
        }
        
        Map<String,String> map = new HashMap<String,String>(aliases.size());
        for (String alias : aliases) {
            map.put(alias.toUpperCase(), alias);
        }
        
        this.aliases = new HashSet<String>(map.values());
    }
    
    public int getCellEntityId() {
    	return cellEntityId;
    }
    
    public void setCellEntityId(int cellEntityId) {
        this.cellEntityId = cellEntityId;
    }

    public long getUniqueCellId() {
        return uniqueCellId;
    }

    public void setUniqueCellId(long uniqueCellId) {
        this.uniqueCellId = uniqueCellId;
    }

    public String getUniqueCellNameAllCaps() {
        return uniqueCellName.toUpperCase();
    }

    public String getStandardSymbol() {
        return getUniqueCellNameAllCaps();
    }

    public void setUniqueCellName(String uniqueCellName) {
        this.uniqueCellName = uniqueCellName;
    }
    
    public String getUniqueCellName() {
        return uniqueCellName;
    }
    
    @Override
    public String toString() {
        return this.getUniqueCellNameAllCaps();
    }

    @Override
    public boolean equals(Object obj0) {
        if (!(obj0 instanceof CanonicalCell)) {
            return false;
        }
        
        CanonicalCell cell0 = (CanonicalCell) obj0;
        if (cell0.getCellEntityId() == cellEntityId) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return (int) uniqueCellId;
    }
}
