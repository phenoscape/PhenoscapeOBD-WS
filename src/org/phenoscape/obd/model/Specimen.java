package org.phenoscape.obd.model;

public class Specimen {

    private final Term collection;
    private final String catalogNumber;

    public Specimen(Term collection, String catalogNumber) {
        this.collection = collection;
        this.catalogNumber = catalogNumber;
    }

    public Term getCollection() {
        return collection;
    }

    public String getCatalogNumber() {
        return catalogNumber;
    }

}
