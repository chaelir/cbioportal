/* LICENSE_TBD */

package org.mskcc.cbio.portal.model;

import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Class for cell_profile
 */
public class CellProfile implements Serializable {
    private int cellProfileId;
    private String stableId;
    private int cancerStudyId;
    private CellAlterationType cellAlterationType;
    private String datatype;
    private String profileName;
    private String profileDescription;
    private String targetLine;
    private boolean showProfileInAnalysisTab;
    private Properties otherMetadataFields;

    public CellProfile() {
      super();
   }

   public CellProfile(String stableId, int cancerStudyId, CellAlterationType cellAlterationType,
						 String datatype, String profileName, String profileDescription, boolean showProfileInAnalysisTab) {
      this();
      this.stableId = stableId;
      this.cancerStudyId = cancerStudyId;
      this.cellAlterationType = cellAlterationType;
      this.datatype = datatype;
      this.profileName = profileName;
      this.profileDescription = profileDescription;
      this.showProfileInAnalysisTab = showProfileInAnalysisTab;
   }

   /**
    * Constructs a new cell profile object with the same attributes as the one given as an argument.
    *
    * @param template  the object to copy
    */
   public CellProfile(CellProfile template) {
       this(
               template.getStableId(),
               template.getCancerStudyId(),
               template.getCellAlterationType(),
               template.getDatatype(),
               template.getProfileName(),
               template.getProfileDescription(),
               template.showProfileInAnalysisTab());
       this.setCellProfileId(template.cellProfileId);
       this.setTargetLine(template.getTargetLine());
       this.setOtherMetadataFields(template.getAllOtherMetadataFields());
   }

   public int getCellProfileId() {
        return cellProfileId;
    }

    public void setCellProfileId(int cellProfileId) {
        this.cellProfileId = cellProfileId;
    }

    public String getStableId() {
        return stableId;
    }

    public void setStableId(String stableId) {
        this.stableId = stableId;
    }

    public int getCancerStudyId() {
        return cancerStudyId;
    }

    public void setCancerStudyId(int cancerStudyId) {
        this.cancerStudyId = cancerStudyId;
    }

    public CellAlterationType getCellAlterationType() {
        return cellAlterationType;
    }

    public void setCellAlterationType(CellAlterationType cellAlterationType) {
        this.cellAlterationType = cellAlterationType;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getProfileName() {
        return profileName;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }

    public String getProfileDescription() {
        return profileDescription;
    }

    public void setProfileDescription(String profileDescription) {
        this.profileDescription = profileDescription;
    }

    public String getTargetLine() {
        return targetLine;
    }

    public void setTargetLine(String targetLine) {
        this.targetLine = targetLine;
    }

    public boolean showProfileInAnalysisTab() {
        return showProfileInAnalysisTab;
    }

    public void setShowProfileInAnalysisTab(boolean showProfileInAnalysisTab) {
        this.showProfileInAnalysisTab = showProfileInAnalysisTab;
    }

    /**
     * Stores metadata fields only recognized in particular data file types.
     *
     * @param fields  a properties instance holding the keys and values
     */
    public void setOtherMetadataFields(Properties fields) {
        this.otherMetadataFields = fields;
    }

    /**
     * Returns all file-specific metadata fields as a Properties object.
     *
     * @return  a properties instance holding the keys and values or null
     */
    public Properties getAllOtherMetadataFields() {
        return this.otherMetadataFields;
    }

    /**
     * Retrieves metadata fields specific to certain data file types.
     *
     * @param fieldname  the name of the field to retrieve
     * @return  the value of the field or null
     */
    public String getOtherMetaDataField(String fieldname) {
        if (otherMetadataFields == null) {
            return null;
        } else {
            return otherMetadataFields.getProperty(fieldname);
        }
    }

    @Override
    public String toString() {
       return ToStringBuilder.reflectionToString(this);
    }

}
