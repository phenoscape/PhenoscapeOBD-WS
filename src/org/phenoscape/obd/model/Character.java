package org.phenoscape.obd.model;

public class Character extends SimpleTerm {
    
    private final String number;

    public Character(String uid, String label, String number) {
        super(uid, label);
        this.number = number;
    }
   
    public String getNumber() {
        return this.number;
    }

}
