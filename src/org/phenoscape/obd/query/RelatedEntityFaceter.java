package org.phenoscape.obd.query;


public abstract class RelatedEntityFaceter extends EntityFaceter {

    public RelatedEntityFaceter(PhenoscapeDataStore dataStore, int minimum, int maximum) {
        super(dataStore, minimum, maximum);
    }

}
