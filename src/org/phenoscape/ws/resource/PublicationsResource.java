package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.query.TaxonAnnotationsQueryConfig;
import org.phenoscape.obd.query.TaxonAnnotationsQueryConfig.SORT_COLUMN;

public class PublicationsResource extends TaxonAnnotationQueryingResource<Term> {

    @Override
    protected int queryForItemsCount(TaxonAnnotationsQueryConfig config)
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected List<Term> queryForItemsSubset(TaxonAnnotationsQueryConfig config)
            throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected JSONObject translateToJSON(Term item) throws JSONException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String translateToText(Term item) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String getItemsKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected SORT_COLUMN getSortColumn() {
        // TODO Auto-generated method stub
        return null;
    }

}
