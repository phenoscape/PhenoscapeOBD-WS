package org.phenoscape.obd.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.phenoscape.obd.model.Vocab.OBO;

public abstract class EntityFaceter extends Faceter {
    
    private static final List<String> rootEntities = new ArrayList<String>();
    static {
        rootEntities.add("TAO:0100000"); // teleost anatomical entity
        rootEntities.add("GO:0008150"); // biological process
        rootEntities.add("GO:0005575"); // cellular component
        rootEntities.add("GO:0003674"); // molecular function
        //TODO need to add spatial
    }

    public EntityFaceter(PhenoscapeDataStore dataStore, int optimalSize) {
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
