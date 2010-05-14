package org.phenoscape.obd.model;

public class Synonym {
    
    private String label;
    private String language;
    
    public String getLabel() {
        return this.label;
    }
    
    /**
     * Set the label for this synonym, which may contain an optional language code 
     * (such as "@en") which will be parsed and stored separately.
     */
    public void setLabel(String label) {
        final int langMarker = label.lastIndexOf("@");
        if (langMarker > 0) {
            this.label = label.substring(0, langMarker);
            if (label.length() > langMarker + 1) {
                this.language = label.substring(langMarker + 1);   
            }
        } else {
            this.label = label;    
        }
    }
    
    public String getLanguage() {
        return this.language;
    }

}
