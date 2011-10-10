package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.Phenotype;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class DistinctPhenotypesResource extends AnnotationQueryingResource<Phenotype> {

    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("entity", SORT_COLUMN.ENTITY);
        COLUMNS.put("quality", SORT_COLUMN.QUALITY);
        COLUMNS.put("relatedentity", SORT_COLUMN.RELATED_ENTITY);
    }

    @Override
    protected JSONObject translateToJSON(Phenotype phenotype) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject entity = this.createBasicJSONTerm(phenotype.getEntity());
        json.put("entity", entity);
        final JSONObject quality = this.createBasicJSONTerm(phenotype.getQuality());
        json.put("quality", quality);
        if (phenotype.getRelatedEntity() != null) {
            final JSONObject relatedEntity = this.createBasicJSONTerm(phenotype.getRelatedEntity());
            json.put("related_entity", relatedEntity);
        }
        return json;
    }

    @Override
    protected String translateToText(Phenotype item) {
        final StringBuffer buffer = new StringBuffer();
        final String tab = "\t";
        buffer.append(item.getEntity().getUID());
        buffer.append(tab);
        buffer.append(item.getEntity().getLabel());
        buffer.append(tab);
        buffer.append(item.getQuality().getUID());
        buffer.append(tab);
        buffer.append(item.getQuality().getLabel());
        buffer.append(tab);
        buffer.append(item.getRelatedEntity() != null ? item.getRelatedEntity().getUID() : "");
        buffer.append(tab);
        buffer.append(item.getRelatedEntity() != null ? item.getRelatedEntity().getLabel() : "");
        return buffer.toString();
    }

    @Override
    protected List<Phenotype> queryForItemsSubset(AnnotationsQueryConfig config) throws SQLException, SolrServerException {
        return this.getDataStore().getDistinctPhenotypesSolr(config).getList();
    }

    @Override
    protected long queryForItemsCount(AnnotationsQueryConfig config) throws SQLException, SolrServerException {
        config.setLimit(0);
        return this.getDataStore().getDistinctPhenotypesSolr(config).getTotal();
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
