package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.TaxonAnnotation;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class TaxonAnnotationsResource extends AnnotationQueryingResource<TaxonAnnotation> {

    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("taxon", SORT_COLUMN.TAXON);
        COLUMNS.put("entity", SORT_COLUMN.ENTITY);
        COLUMNS.put("quality", SORT_COLUMN.QUALITY);
        COLUMNS.put("relatedentity", SORT_COLUMN.RELATED_ENTITY);
    }

    @Override
    protected long queryForItemsCount(AnnotationsQueryConfig config) throws SQLException, SolrServerException {
        config.setLimit(0);
        return this.getDataStore().getDistinctTaxonAnnotationsSolr(config).getTotal();
        //return this.getDataStore().getCountOfDistinctTaxonomicAnnotations(config);
    }

    @Override
    protected List<TaxonAnnotation> queryForItemsSubset(AnnotationsQueryConfig config) throws SQLException, SolrServerException {
        return this.getDataStore().getDistinctTaxonAnnotationsSolr(config).getList();
    }

    @Override
    protected JSONObject translateToJSON(TaxonAnnotation annotation) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject taxon = this.createBasicJSONTerm(annotation.getTaxon());
        taxon.put("extinct", annotation.getTaxon().isExtinct());
        if (annotation.getTaxon().getRank() != null) {
            final JSONObject rank = this.createBasicJSONTerm(annotation.getTaxon().getRank());
            taxon.put("rank", rank);
        }
        json.put("taxon", taxon);
        final JSONObject entity = this.createBasicJSONTerm(annotation.getEntity());
        json.put("entity", entity);
        final JSONObject quality = this.createBasicJSONTerm(annotation.getQuality());
        json.put("quality", quality);
        if (annotation.getRelatedEntity() != null) {
            final JSONObject relatedEntity = this.createBasicJSONTerm(annotation.getRelatedEntity());
            json.put("related_entity", relatedEntity);
        }
        return json;    
    }

    @Override
    protected String translateToText(TaxonAnnotation annotation) {
        final StringBuffer buffer = new StringBuffer();
        final String tab = "\t";
        buffer.append(annotation.getTaxon().getUID());
        buffer.append(tab);
        buffer.append(annotation.getTaxon().getLabel());
        buffer.append(tab);
        buffer.append(annotation.getEntity().getUID());
        buffer.append(tab);
        buffer.append(annotation.getEntity().getLabel());
        buffer.append(tab);
        buffer.append(annotation.getQuality().getUID());
        buffer.append(tab);
        buffer.append(annotation.getQuality().getLabel());
        buffer.append(tab);
        buffer.append(annotation.getRelatedEntity() != null ? annotation.getRelatedEntity().getUID() : "");
        buffer.append(tab);
        buffer.append(annotation.getRelatedEntity() != null ? annotation.getRelatedEntity().getLabel() : "");
        return buffer.toString();
    }

    @Override
    protected String getItemsKey() {
        return "annotations";
    }

    @Override
    protected SORT_COLUMN getDefaultSortColumn() {
        return SORT_COLUMN.TAXON;
    }

    @Override
    protected Map<String, SORT_COLUMN> getSortColumns() {
        return COLUMNS;
    }


}
