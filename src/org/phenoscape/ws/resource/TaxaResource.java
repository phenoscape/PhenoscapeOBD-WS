package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.TaxonTerm;
import org.phenoscape.obd.query.TaxonAnnotationsQueryConfig;
import org.phenoscape.obd.query.TaxonAnnotationsQueryConfig.SORT_COLUMN;

public class TaxaResource extends TaxonAnnotationQueryingResource<TaxonTerm> {
    
    //TODO allow other sorting
    private SORT_COLUMN sortColumn = SORT_COLUMN.TAXON;

    @Override
    protected int queryForItemsCount(TaxonAnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getCountOfAnnotatedTaxa(config);
    }

    @Override
    protected List<TaxonTerm> queryForItemsSubset(TaxonAnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getAnnotatedTaxa(config);
    }

    @Override
    protected JSONObject translateToJSON(TaxonTerm taxon) throws JSONException {
        //TODO should rank terms for order and class be included? seems redundant
        final JSONObject json = new JSONObject();
        json.put("id", taxon.getUID());
        json.put("name", taxon.getLabel());
        json.put("extinct", taxon.isExtinct());
        if (taxon.getRank() != null) {
            final JSONObject rank = new JSONObject();
            rank.put("id", taxon.getRank().getUID());
            rank.put("name", taxon.getRank().getLabel());
            json.put("rank", rank);
        }
        if (taxon.getTaxonomicFamily() != null) {
            final JSONObject taxonomicFamily = new JSONObject();
            taxonomicFamily.put("id", taxon.getTaxonomicFamily().getUID());
            taxonomicFamily.put("name", taxon.getTaxonomicFamily().getLabel());
            taxonomicFamily.put("extinct", taxon.getTaxonomicFamily().isExtinct());
//            if (taxon.getRank() != null) {
//                final JSONObject rank = new JSONObject();
//                rank.put("id", taxon.getTaxonomicClass().getRank().getUID());
//                rank.put("name", taxon.getTaxonomicClass().getRank().getLabel());
//                taxonomicClass.put("rank", rank);
//            }
            json.put("class", taxonomicFamily);
        }
        if (taxon.getTaxonomicOrder() != null) {
            final JSONObject taxonomicOrder = new JSONObject();
            taxonomicOrder.put("id", taxon.getTaxonomicOrder().getUID());
            taxonomicOrder.put("name", taxon.getTaxonomicOrder().getLabel());
            taxonomicOrder.put("extinct", taxon.getTaxonomicOrder().isExtinct());
//            if (taxon.getRank() != null) {
//                final JSONObject rank = new JSONObject();
//                rank.put("id", taxon.getTaxonomicOrder().getRank().getUID());
//                rank.put("name", taxon.getTaxonomicOrder().getRank().getLabel());
//                taxonomicOrder.put("rank", rank);
//            }
            json.put("order", taxonomicOrder);
        }
        return json;    
    }

    @Override
    protected String translateToText(TaxonTerm taxon) {
        //TODO rank, etc.
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
    protected SORT_COLUMN getSortColumn() {
        return this.sortColumn;
    }

}
