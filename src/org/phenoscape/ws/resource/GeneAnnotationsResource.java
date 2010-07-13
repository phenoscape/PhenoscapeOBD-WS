package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.GeneAnnotation;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.query.GeneAnnotationsQueryConfig;
import org.phenoscape.obd.query.QueryException;
import org.phenoscape.obd.query.GeneAnnotationsQueryConfig.SORT_COLUMN;
import org.phenoscape.ws.representation.StreamableJSONRepresentation;
import org.phenoscape.ws.representation.StreamableTextRepresentation;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class GeneAnnotationsResource extends AbstractPhenoscapeResource {

    private static final Map<String,SORT_COLUMN> COLUMNS = new HashMap<String,SORT_COLUMN>();
    static {
        COLUMNS.put("gene", SORT_COLUMN.GENE);
        COLUMNS.put("entity", SORT_COLUMN.ENTITY);
        COLUMNS.put("quality", SORT_COLUMN.QUALITY);
        COLUMNS.put("relatedentity", SORT_COLUMN.RELATED_ENTITY);
    }
    /**
     * The maximum number of annotations to pull out of the database in one query.
     * TODO: need to investigate ideal value for this to maximize performance with acceptable memory usage
     */
    private static final int QUERY_LIMIT = 1000;
    private JSONObject query = new JSONObject();
    private String sortColumn = "gene";
    private int limit;
    private int index;
    private boolean sortDescending;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        final String jsonQuery = this.getFirstQueryValue("query");
        if (jsonQuery != null) {
            try {
                this.query = new JSONObject(jsonQuery);
            } catch (JSONException e) {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e);
            }
        }
        final String column = this.getFirstQueryValue("sortby");
        if (column != null) {
            if (COLUMNS.keySet().contains(column)) {
                this.sortColumn = column;
            } else {
                throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Invalid sort column");
            }
        }
        this.limit = this.getIntegerQueryValue("limit", 0);
        this.index = this.getIntegerQueryValue("index", 0);
        this.sortDescending = this.getBooleanQueryValue("desc", false);
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final Iterator<JSONObject> annotations = this.translateToJSON(this.queryForAnnotations());
            final int total = this.queryForAnnotationsCount(this.createInitialQueryConfig());
            final JSONObject otherValues = new JSONObject();
            otherValues.put("total", total);
            return new StreamableJSONRepresentation(annotations, "annotations", otherValues);
        } catch (JSONException e) {
            this.log().error("Error creating JSON object", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            this.log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (QueryException e) {
            this.log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    @Get("tsv")
    public Representation getTabDelimitedRepresentation() {
        try {
            final Iterator<String> annotations = this.translateToText(this.queryForAnnotations());
            return new StreamableTextRepresentation(annotations, MediaType.TEXT_TSV);
        } catch (JSONException e) {
            this.log().error("Invalid annotation query", e);
            this.setStatus(Status.CLIENT_ERROR_BAD_REQUEST, e);
            return null;
        } catch (SQLException e) {
            this.log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (QueryException e) {
            this.log().error("Error querying database", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;  
        }
    }

    private Iterator<JSONObject> translateToJSON(final Iterator<GeneAnnotation> annotations) {
        return new Iterator<JSONObject>() {
            @Override
            public boolean hasNext() {
                return annotations.hasNext();
            }
            @Override
            public JSONObject next() {
                try {
                    return translateToJSON(annotations.next());
                } catch (JSONException e) {
                    log().error("Could not create JSON object from annotation", e);
                    return new JSONObject();
                }
            }
            @Override
            public void remove() {
                annotations.remove();
            }
        };
    }

    private JSONObject translateToJSON(GeneAnnotation annotation) throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject gene = new JSONObject();
        gene.put("id", annotation.getGene().getUID());
        gene.put("name", annotation.getGene().getLabel());
        json.put("gene", gene);
        final JSONObject entity = new JSONObject();
        entity.put("id", annotation.getEntity().getUID());
        entity.put("name", annotation.getEntity().getLabel());
        json.put("entity", entity);
        final JSONObject quality = new JSONObject();
        quality.put("id", annotation.getQuality().getUID());
        quality.put("name", annotation.getQuality().getLabel());
        json.put("quality", quality);
        final JSONObject relatedEntity = new JSONObject();
        relatedEntity.put("id", annotation.getRelatedEntity().getUID());
        relatedEntity.put("name", annotation.getRelatedEntity().getLabel());
        json.put("related_entity", relatedEntity);
        return json;
    }

    private Iterator<String> translateToText(final Iterator<GeneAnnotation> annotations) {
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return annotations.hasNext();
            }
            @Override
            public String next() {
                return translateToText(annotations.next());
            }
            @Override
            public void remove() {
                annotations.remove();
            }
        };
    }

    private String translateToText(GeneAnnotation annotation) {
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

    /**
     * @throws JSONException
     * @throws SQLException
     * @throws QueryException
     */
    private Iterator<GeneAnnotation> queryForAnnotations() throws JSONException, SQLException, QueryException {
        final GeneAnnotationsQueryConfig config = this.createInitialQueryConfig();
        final List<GeneAnnotation> initialResults = this.getDataStore().getGeneAnnotations(config);
        return new Iterator<GeneAnnotation>() {
            private Iterator<GeneAnnotation> currentAnnotations = initialResults.iterator();
            private int gotten = Math.max(initialResults.size(), QUERY_LIMIT);
            @Override
            public boolean hasNext() {
                if (this.currentAnnotations.hasNext()) {
                    return true; 
                } else if (this.needMore()) {
                    config.setIndex(config.getIndex() + QUERY_LIMIT);
                    try {
                        List<GeneAnnotation> nextResults = getDataStore().getGeneAnnotations(config);
                        this.gotten += QUERY_LIMIT;
                        this.currentAnnotations = nextResults.iterator();
                        return this.currentAnnotations.hasNext();
                    } catch (SQLException e) {
                        throw new QueryException(e);
                    }
                } else {
                    return false;
                }
            }
            @Override
            public GeneAnnotation next() {
                return this.currentAnnotations.next();
            }
            @Override
            public void remove() {}
            private boolean needMore() {
                if (limit > 0) {
                    return this.gotten < limit;       
                } else {
                    return true;
                }
            }
        };
    }

    private int queryForAnnotationsCount(GeneAnnotationsQueryConfig config) throws SQLException {
        return this.getDataStore().getCountOfGeneAnnotations(config);
    }

    private GeneAnnotationsQueryConfig createInitialQueryConfig() throws JSONException, QueryException {
        final GeneAnnotationsQueryConfig config = new GeneAnnotationsQueryConfig();
        config.setIndex(this.index);
        config.setSortColumn(COLUMNS.get(this.sortColumn));
        config.setSortDescending(this.sortDescending);
        if (this.limit > 0) {
            config.setLimit(Math.min(this.limit, QUERY_LIMIT));
        } else {
            config.setLimit(QUERY_LIMIT);
        }
        if (this.query.has("gene")) {
            for (JSONObject gene : this.toIterable(this.query.getJSONArray("gene"))) {
                config.addGeneID(gene.getString("id"));
            } 
        }
        if (this.query.has("phenotype")) {
            for (JSONObject phenotype : this.toIterable(this.query.getJSONArray("phenotype"))) {
                final PhenotypeSpec spec = new PhenotypeSpec();
                if (phenotype.has("entity")) {
                    final JSONObject entity = phenotype.getJSONObject("entity");
                    spec.setEntityID(entity.getString("id"));
                    if (entity.has("including_parts")) {
                        spec.setIncludeEntityParts(entity.getBoolean("including_parts"));
                    }
                }
                if (phenotype.has("quality")) {
                    final JSONObject quality = phenotype.getJSONObject("quality");
                    spec.setQualityID(quality.getString("id"));
                }
                if (phenotype.has("related_entity")) {
                    final JSONObject relatedEntity = phenotype.getJSONObject("related_entity");
                    spec.setRelatedEntityID(relatedEntity.getString("id"));
                }
                config.addPhenotype(spec);
            }
        }
        return config;
    }

    /**
     * @throws QueryException
     */
    private Iterable<JSONObject> toIterable(final JSONArray array) throws QueryException {
        return new Iterable<JSONObject>() {
            @Override
            public Iterator<JSONObject> iterator() {
                return new Iterator<JSONObject>() {
                    int index = 0;
                    @Override
                    public boolean hasNext() {
                        return this.index < array.length();
                    }
                    @Override
                    public JSONObject next() {
                        try {
                            final JSONObject json = array.getJSONObject(this.index);
                            this.index++;
                            return json;
                        } catch (JSONException e) {
                            throw new QueryException(e);
                        }
                    }
                    @Override
                    public void remove() {}
                };
            }
        };
    }

}
