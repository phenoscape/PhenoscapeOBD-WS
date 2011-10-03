package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class PublicationsResource extends AnnotationQueryingResource<Term> {
    
    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("publication", SORT_COLUMN.PUBLICATION);
    }

    @Override
    protected long queryForItemsCount(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getCountOfAnnotatedPublications(config);
    }

    @Override
    protected List<Term> queryForItemsSubset(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getAnnotatedPublications(config);
    }

    @Override
    protected JSONObject translateToJSON(Term item) throws JSONException {
        return this.createBasicJSONTerm(item);
    }

    @Override
    protected String translateToText(Term item) {
        final StringBuffer text = new StringBuffer();
        text.append(item.getUID());
        text.append("\t");
        text.append(item.getLabel());
        return text.toString();
    }

    @Override
    protected String getItemsKey() {
        return "publications";
    }

    @Override
    protected SORT_COLUMN getDefaultSortColumn() {
        return SORT_COLUMN.PUBLICATION;
    }

    @Override
    protected Map<String, SORT_COLUMN> getSortColumns() {
        return COLUMNS;
    }

}
