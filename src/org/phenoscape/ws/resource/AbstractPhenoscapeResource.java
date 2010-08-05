package org.phenoscape.ws.resource;

import java.util.Iterator;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.PhenotypeSpec;
import org.phenoscape.obd.model.Term;
import org.phenoscape.obd.query.AnnotationsQueryConfig;
import org.phenoscape.obd.query.PhenoscapeDataStore;
import org.phenoscape.obd.query.QueryException;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.data.Reference;
import org.restlet.resource.ServerResource;

/**
 * An abstract Restlet resource providing functionality needed by any resource, such as creation of query objects and database connections.
 * @author Jim Balhoff
 */
public class AbstractPhenoscapeResource extends ServerResource {
    
    private PhenoscapeDataStore dataStore = null;
    
    /**
     * Get an instance of PhenoscapeDataStore initialized with this application's JDBC connection.
     */
    protected PhenoscapeDataStore getDataStore() {
        if (this.dataStore == null) {
            this.dataStore = new PhenoscapeDataStore(this.getDataSource());
        }
        return this.dataStore;
    }
    
    /**
     * Return first value of the given query parameter, decoded, or null if not present.
     */
    protected String getFirstQueryValue(String parameter) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            return Reference.decode(this.getQuery().getFirstValue(parameter));
        } else {
            return null;
        }
    }
    
    /**
     * Return first value of the given query parameter as a boolean, or the given default value if not present.
     */
    protected boolean getBooleanQueryValue(String parameter, boolean defaultValue) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            final String queryValue = this.getFirstQueryValue(parameter);
            return Boolean.parseBoolean(queryValue);
        } else {
            return defaultValue;
        }
    }
    
    /**
     * Return first value of the given query parameter as an integer, or the given default value if not present.
     */
    protected int getIntegerQueryValue(String parameter, int defaultValue) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            final String queryValue = this.getFirstQueryValue(parameter);
            return Integer.parseInt(queryValue);
        } else {
            return defaultValue;
        }
    }
    
    protected JSONObject getJSONQueryValue(String parameter, JSONObject defaultValue) throws JSONException {
        if (this.getQuery().getFirstValue(parameter) != null) {
            final String queryValue = this.getFirstQueryValue(parameter);
            return new JSONObject(queryValue);
        } else {
            return defaultValue;
        }
    }
    
    protected AnnotationsQueryConfig initializeQueryConfig(JSONObject query) throws JSONException, QueryException {
        final AnnotationsQueryConfig config = new AnnotationsQueryConfig();
        if (query.has("taxon")) {
            for (JSONObject taxon : this.toIterable(query.getJSONArray("taxon"))) {
                config.addTaxonID(taxon.getString("id"));
            } 
        }
        if (query.has("match_all_taxa")) {
            config.setMatchAllTaxa(query.getBoolean("match_all_taxa"));
        }
        if (query.has("gene")) {
            for (JSONObject gene : this.toIterable(query.getJSONArray("gene"))) {
                config.addGeneID(gene.getString("id"));
            } 
        }
        if (query.has("phenotype")) {
            for (JSONObject phenotype : this.toIterable(query.getJSONArray("phenotype"))) {
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
        if (query.has("match_all_phenotypes")) {
            config.setMatchAllPhenotypes(query.getBoolean("match_all_phenotypes"));
        }
        if (query.has("publication")) {
            for (JSONObject publication : this.toIterable(query.getJSONArray("publication"))) {
                config.addPublicationID(publication.getString("id"));
            } 
        }
        if (query.has("match_all_publications")) {
            config.setMatchAllPublications(query.getBoolean("match_all_publications"));
        }
        if (query.has("include_inferred")) {
            config.setIncludeInferredAnnotations(query.getBoolean("include_inferred"));
        }
        return config;
    }
    
    /**
     * @throws QueryException
     */
    protected Iterable<JSONObject> toIterable(final JSONArray array) throws QueryException {
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
    
    protected JSONObject createBasicJSONTerm(Term term) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("id", term.getUID());
        json.put("name", term.getLabel());
        return json;
    }
    
    /**
     * Retrieve the JDBC DataSource from the application context.
     */
    protected DataSource getDataSource() {
        return (DataSource)(this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.DATA_SOURCE_KEY));
    }
    
    protected Logger log() {
        return Logger.getLogger(this.getClass());
    }

}
