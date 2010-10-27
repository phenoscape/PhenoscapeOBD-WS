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
    
    @Override
    public int hashCode() {
        return this.getUID().hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Character) {
            final Character otherCharacter = (Character)other;
            return (this.getUID().equals(otherCharacter.getUID()));
        }
        return false;
    }

}
