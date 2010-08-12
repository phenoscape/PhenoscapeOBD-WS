package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class TaxaResource extends AnnotationQueryingResource<TaxonTerm> {
    
    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("taxon", SORT_COLUMN.TAXON);
        COLUMNS.put("family", SORT_COLUMN.FAMILY);
        COLUMNS.put("order", SORT_COLUMN.ORDER);
    }
    
    @Override
    protected int queryForItemsCount(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getCountOfAnnotatedTaxa(config);
    }

    @Override
    protected List<TaxonTerm> queryForItemsSubset(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getAnnotatedTaxa(config);
    }

    @Override
    protected JSONObject translateToJSON(TaxonTerm taxon) throws JSONException {
        final JSONObject json = this.createBasicJSONTerm(taxon);
        json.put("extinct", taxon.isExtinct());
        if (taxon.getRank() != null) {
            final JSONObject rank = this.createBasicJSONTerm(taxon.getRank());
            json.put("rank", rank);
        }
        if (taxon.getTaxonomicFamily() != null) {
            final JSONObject taxonomicFamily = this.createBasicJSONTerm(taxon.getTaxonomicFamily());
            taxonomicFamily.put("extinct", taxon.getTaxonomicFamily().isExtinct());
            json.put("family", taxonomicFamily);
        }
        if (taxon.getTaxonomicOrder() != null) {
            final JSONObject taxonomicOrder = this.createBasicJSONTerm(taxon.getTaxonomicOrder());
            taxonomicOrder.put("extinct", taxon.getTaxonomicOrder().isExtinct());
            json.put("order", taxonomicOrder);
        }
        return json;    
    }

    @Override
    protected String translateToText(TaxonTerm taxon) {
        final StringBuffer buffer = new StringBuffer();
        final String tab = "\t";
        buffer.append(taxon.getUID());
        buffer.append(tab);
        buffer.append(taxon.getLabel());
        buffer.append(tab);
        buffer.append(taxon.getRank() != null ? taxon.getRank().getUID() : "");
        buffer.append(tab);
        buffer.append(taxon.getRank() != null ? taxon.getRank().getLabel() : "");
        buffer.append(tab);
        buffer.append(taxon.isExtinct() ? "extinct" : "");
        buffer.append(tab);
        buffer.append(taxon.getTaxonomicOrder() != null ? taxon.getTaxonomicOrder().getUID() : "");
        buffer.append(tab);
        buffer.append(taxon.getTaxonomicOrder() != null ? taxon.getTaxonomicOrder().getLabel() : "");
        buffer.append(tab);
        buffer.append(taxon.getTaxonomicFamily() != null ? taxon.getTaxonomicFamily().getUID() : "");
        buffer.append(tab);
        buffer.append(taxon.getTaxonomicFamily() != null ? taxon.getTaxonomicFamily().getLabel() : "");
        return buffer.toString();
    }

    @Override
    protected String getItemsKey() {
        return "taxa";
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
