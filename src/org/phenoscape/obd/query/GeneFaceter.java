package org.phenoscape.obd.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public abstract class GeneFaceter extends Faceter {
    
    private static final List<String> rootEntities = new ArrayList<String>();
    static {
        rootEntities.add("GO:0008150"); // biological process
        rootEntities.add("GO:0005575"); // cellular component
        rootEntities.add("GO:0003674"); // molecular function
    }

    public GeneFaceter(PhenoscapeDataStore dataStore, int optimalSize) {
        super(dataStore, optimalSize);
    }

    @Override
    protected List<String> getChildren(String focalTermUID) throws SQLException {
        if (focalTermUID == null) {
            return rootEntities;
        } else {
            return this.getDataStore().getGeneFacetChildrenUIDs(focalTermUID);
        }
    }

}
