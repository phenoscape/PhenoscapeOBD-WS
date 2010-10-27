package org.phenoscape.obd.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Matrix {
    
    private final Map<OTU, Map<Character, Term>> matrixCells = new HashMap<OTU, Map<Character, Term>>();
    
    public List<OTU> getOTUs() {
        return new ArrayList<OTU>(this.matrixCells.keySet());
    }
    
    public List<Character> getCharacters() {
        final Set<Character> characters = new HashSet<Character>();
        for (Map<Character, Term> characterMap : this.matrixCells.values()) {
            characters.addAll(characterMap.keySet());
        }
        return new ArrayList<Character>(characters);
    }
    
    public Term getState(OTU otu, Character character) {
        if (this.matrixCells.containsKey(otu)) {
            return this.matrixCells.get(otu).get(character);
        }
        return null;
    }
    
    public void setState(OTU otu, Character character, Term state) {
        if (!this.matrixCells.containsKey(otu)) {
            this.matrixCells.put(otu, new HashMap<Character, Term>());
        }
        this.matrixCells.get(otu).put(character, state);
    }

}
