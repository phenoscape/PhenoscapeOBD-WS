package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.GeneAnnotation;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.AnnotationsQueryConfig.SORT_COLUMN;

public class GeneAnnotationsResource extends AnnotationQueryingResource<GeneAnnotation> {

    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("gene", SORT_COLUMN.GENE);
        COLUMNS.put("entity", SORT_COLUMN.ENTITY);
        COLUMNS.put("quality", SORT_COLUMN.QUALITY);
        COLUMNS.put("relatedentity", SORT_COLUMN.RELATED_ENTITY);
    }

    @Override
    protected JSONObject translateToJSON(GeneAnnotation annotation) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject gene = this.createBasicJSONTerm(annotation.getGene());
        json.put("gene", gene);
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
    protected String translateToText(GeneAnnotation annotation) {
        final StringBuffer buffer = new StringBuffer();
        final String tab = "\t";
        buffer.append(annotation.getGene().getUID());
        buffer.append(tab);
        buffer.append(annotation.getGene().getLabel());
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
    protected SORT_COLUMN getDefaultSortColumn() {
        return SORT_COLUMN.GENE;
    }

    @Override
    protected String getItemsKey() {
        return "annotations";
    }

    @Override
    protected Map<String, SORT_COLUMN> getSortColumns() {
        return COLUMNS;
    }

    @Override
    protected long queryForItemsCount(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getCountOfGeneAnnotations(config);
    }

    @Override
    protected List<GeneAnnotation> queryForItemsSubset(AnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getGeneAnnotations(config);
    }

}
