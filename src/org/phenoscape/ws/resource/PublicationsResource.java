package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class PublicationsResource extends AnnotationQueryingResource<Term> {

    @Override
    protected int queryForItemsCount(AnnotationsQueryConfig config)
            throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected List<Term> queryForItemsSubset(AnnotationsQueryConfig config)
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
    protected SORT_COLUMN getDefaultSortColumn() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Map<String, SORT_COLUMN> getSortColumns() {
        // TODO Auto-generated method stub
        return null;
    }

}
