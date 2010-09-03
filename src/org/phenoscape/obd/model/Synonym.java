package org.phenoscape.obd.model;

public class Synonym {
    
    private String label;
    private String language;
    public static enum SCOPE { BROAD, NARROW, RELATED, EXACT }
    private SCOPE scope = null;
    private Term type = null;
    
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

    public SCOPE getScope() {
        return this.scope;
    }
    
    public void setScope(SCOPE scope) {
        this.scope = scope;
    }
    
    public Term getType() {
        return this.type;
    }
    
    public void setType(Term type) {
        this.type = type;
    }
    
}
