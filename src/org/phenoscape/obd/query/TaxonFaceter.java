package org.phenoscape.obd.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.phenoscape.obd.model.Vocab.OBO;

public abstract class TaxonFaceter extends Faceter {

    private static final List<String> rootEntities = new ArrayList<String>();
    static {
        rootEntities.add("TTO:0"); // Chordata
    }

    public TaxonFaceter(PhenoscapeDataStore dataStore, int optimalSize) {
        super(dataStore, optimalSize);
    }

    @Override
    protected List<String> getChildren(String focalTermUID) throws SQLException {
        if (focalTermUID == null) {
            return rootEntities;
        } else {
            return this.getDataStore().getChildrenUIDs(focalTermUID, OBO.IS_A);
        }
    }
    
}
