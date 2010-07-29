package org.phenoscape.obd.query;


public class GeneAnnotationsQueryConfig extends AnnotationsQueryConfig {

    public GeneAnnotationsQueryConfig() {
        super();
        this.setSortColumn(SORT_COLUMN.GENE);
    }

}
