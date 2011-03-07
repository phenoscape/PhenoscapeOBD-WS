package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class DistinctPhenotypesResource extends AnnotationQueryingResource<String> {
    
    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("entity", SORT_COLUMN.ENTITY);
        COLUMNS.put("quality", SORT_COLUMN.QUALITY);
        COLUMNS.put("relatedentity", SORT_COLUMN.RELATED_ENTITY);
    }

    @Override
    protected JSONObject translateToJSON(String item) throws JSONException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String translateToText(String item) {
        // TODO Auto-generated method stub
        return item;
    }

    @Override
    protected List<String> queryForItemsSubset(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getDistinctPhenotypes(config);
    }

    @Override
    protected int queryForItemsCount(AnnotationsQueryConfig config) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected String getItemsKey() {
        return "phenotypes";
    }

    @Override
    protected SORT_COLUMN getDefaultSortColumn() {
        return SORT_COLUMN.ENTITY;
    }

    @Override
    protected Map<String, SORT_COLUMN> getSortColumns() {
        return COLUMNS;
    }

}
