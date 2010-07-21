package org.phenoscape.ws.resource;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.query.QueryException;
import org.phenoscape.obd.query.TaxonAnnotationsQueryConfig;
import org.phenoscape.ws.representation.StreamableJSONRepresentation;
import org.phenoscape.ws.representation.StreamableTextRepresentation;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public abstract class TaxonAnnotationQueryingResource<T> extends AbstractPhenoscapeResource {

    /**
     * The maximum number of annotations to pull out of the database in one query.
     * TODO: need to investigate ideal value for this to maximize performance with acceptable memory usage
     */
    private static final int QUERY_LIMIT = 50000;
    private JSONObject query = new JSONObject();
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
        this.limit = this.getIntegerQueryValue("limit", 0);
        this.index = this.getIntegerQueryValue("index", 0);
        this.sortDescending = this.getBooleanQueryValue("desc", false);
    }
    
    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            final Iterator<JSONObject> annotations = this.translateToJSON(this.queryForItems());
            final int total = this.queryForItemsCount(this.createInitialQueryConfig());
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
            final Iterator<String> annotations = this.translateToText(this.queryForItems());
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

    protected final Iterator<JSONObject> translateToJSON(final Iterator<T> annotations) {
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

    protected abstract JSONObject translateToJSON(T item) throws JSONException;

    protected final Iterator<String> translateToText(final Iterator<T> items) {
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return items.hasNext();
            }
            @Override
            public String next() {
                return translateToText(items.next());
            }
            @Override
            public void remove() {
                items.remove();
            }
        };
    }

    protected abstract String translateToText(T item);

    /**
     * @throws JSONException
     * @throws SQLException
     * @throws QueryException
     */
    protected final Iterator<T> queryForItems() throws JSONException, SQLException, QueryException {
        final TaxonAnnotationsQueryConfig config = this.createInitialQueryConfig();
        final List<T> initialResults = this.queryForItemsSubset(config);
        return new Iterator<T>() {
            private Iterator<T> currentAnnotations = initialResults.iterator();
            private int gotten = QUERY_LIMIT;
            private boolean stop = initialResults.size() < QUERY_LIMIT;
            @Override
            public boolean hasNext() {
                if (this.currentAnnotations.hasNext()) {
                    return true; 
                } else if (this.needMore()) {
                    config.setIndex(config.getIndex() + QUERY_LIMIT);
                    try {
                        List<T> nextResults = queryForItemsSubset(config);
                        this.gotten += QUERY_LIMIT;
                        this.stop = nextResults.size() < QUERY_LIMIT;
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
            public T next() {
                return this.currentAnnotations.next();
            }
            @Override
            public void remove() {}
            private boolean needMore() {
                if (stop) {
                    return false;
                } else if (limit > 0) {
                    return this.gotten < limit;       
                } else {
                    return true;
                }
            }
        };
    }
    
    protected abstract List<T> queryForItemsSubset(TaxonAnnotationsQueryConfig config) throws SQLException;

    protected abstract int queryForItemsCount(TaxonAnnotationsQueryConfig config) throws SQLException;

    private TaxonAnnotationsQueryConfig createInitialQueryConfig() throws JSONException, QueryException {
        //TODO inferred
        final TaxonAnnotationsQueryConfig config = new TaxonAnnotationsQueryConfig();
        config.setIndex(this.index);
        //TODO config.setSortColumn(COLUMNS.get(this.sortColumn));
        config.setSortDescending(this.sortDescending);
        if (this.limit > 0) {
            config.setLimit(Math.min(this.limit, QUERY_LIMIT));
        } else {
            config.setLimit(QUERY_LIMIT);
        }
        if (this.query.has("taxon")) {
            for (JSONObject gene : this.toIterable(this.query.getJSONArray("taxon"))) {
                config.addTaxonID(gene.getString("id"));
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

}
