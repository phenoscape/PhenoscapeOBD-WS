package org.obd.ws.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class AnnotationSourceResource extends AbstractOBDResource {

    String taxonID = null;
    String entityID = null;
    String qualityID = null;
    String relatedEntityID = null;

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.taxonID = this.getFirstQueryValue("taxon");
        this.entityID = this.getFirstQueryValue("entity");
        this.qualityID = this.getFirstQueryValue("quality");
        this.relatedEntityID = this.getFirstQueryValue("relatedentity");
    }

    @Get("json")
    public Representation getJSONRepresentation() {
        try {
            return new JsonRepresentation(this.queryAnnotationSourceData());
        } catch (JSONException e) {
            log().error("Failed to create JSON object", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (SQLException e) {
            log().error("Database error querying for annotation source", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        } catch (ClassNotFoundException e) {
            log().error("Error querying for annotation source", e);
            this.setStatus(Status.SERVER_ERROR_INTERNAL, e);
            return null;
        }
    }

    private JSONObject queryAnnotationSourceData() throws SQLException, ClassNotFoundException, JSONException {
        final JSONObject json = new JSONObject();
        Connection connection = null;
        try {
            this.connectShardToDatabase();
            connection = this.getShard().getConnection();
            PreparedStatement statement = null;
            ResultSet result = null;
            try {
                final String sourceQuery = 
                    "SELECT DISTINCT taxon_uid, taxon_label, taxon_rank_uid, taxon_rank_label, taxon_is_extinct, queryable_annotation.entity_uid, dw_entity_table.entity_label, quality_uid, quality_label, related_entity_uid, related_entity_label, publication_uid, pub.authors, pub.publication_year AS year, pub.title, pub.secondary_title, pub.volume, pub.pages, character_number, character_label, state_label " +
                    "FROM queryable_annotation " +
                    "JOIN dw_entity_table ON (dw_entity_table.entity_nid = queryable_annotation.entity_node_id) " +
                    "JOIN dw_publication_table pub ON (queryable_annotation.publication_uid = pub.publication) " +
                    "WHERE taxon_uid = ? " +
                    "AND queryable_annotation.entity_uid = ? " +
                    "AND quality_uid = ? " + 
                    "AND related_entity_uid " + ((this.relatedEntityID != null) ? "= ?" : "IS NULL");
                statement = connection.prepareStatement(sourceQuery);
                statement.setString(1, this.taxonID);
                statement.setString(2, this.entityID);
                statement.setString(3, this.qualityID);
                if (this.relatedEntityID != null) { statement.setString(4, this.relatedEntityID); }
                result = statement.executeQuery();
                final JSONArray sources = new JSONArray();
                while (result.next()) {
                    if (result.isFirst()) {
                        json.put("phenotype", this.buildAnnotation(result));
                    }
                    sources.put(this.buildSource(result));
                }
                json.put("sources", sources);
            } finally {
                if (statement != null) { statement.close(); }
            }
        } finally {
            this.disconnectShardFromDatabase();
        }
        return json;
    }
    
    private JSONObject buildAnnotation(ResultSet result) throws JSONException, SQLException {
        final JSONObject annotation = new JSONObject();
        final JSONObject subject = new JSONObject();
        subject.put("id", result.getString("taxon_uid"));
        subject.put("name", result.getString("taxon_label"));
        if (result.getString("taxon_rank_uid") != null) {
            final JSONObject rank = new JSONObject();
            rank.put("id", result.getString("taxon_rank_uid"));
            rank.put("name", result.getString("taxon_rank_label"));
            subject.put("rank", rank);
        }
        subject.put("extinct", result.getBoolean("taxon_is_extinct"));
        annotation.put("subject", subject);
        final JSONObject entity = new JSONObject();
        entity.put("id", result.getString("entity_uid"));
        entity.put("name", result.getString("entity_label"));
        annotation.put("entity", entity);
        final JSONObject quality = new JSONObject();
        quality.put("id", result.getString("quality_uid"));
        quality.put("name", result.getString("quality_label"));
        annotation.put("quality", quality);
        if (this.relatedEntityID != null) {
            final JSONObject relatedEntity = new JSONObject();
            relatedEntity.put("id", result.getString("related_entity_uid"));
            relatedEntity.put("name", result.getString("related_entity_label"));
            annotation.put("related_entity", relatedEntity);
        }
        return annotation;
    }
    
    private JSONObject buildSource(ResultSet result) throws SQLException, JSONException {
        final JSONObject source = new JSONObject();
        source.put("publication", this.buildCitation(result));
        source.put("character_text", result.getString("character_label"));
        source.put("character_number", result.getString("character_number"));
        source.put("state_text", result.getString("state_label"));
        source.put("curated_by", ""); //TODO
        return source;
    }
    
    private String buildCitation(ResultSet result) throws SQLException {
        final StringBuffer citation = new StringBuffer();
        final String authors = result.getString("authors");
        citation.append(authors);
        if (!authors.endsWith(".")) { citation.append("."); }
        citation.append(" ");
        citation.append(result.getString("year"));
        citation.append(". ");
        final String title = result.getString("title");
        citation.append(title);
        if (!title.endsWith(".")) { citation.append("."); }
        citation.append(" ");
        if (result.getString("secondary_title") != null) {
            citation.append(" ");
            citation.append(result.getString("secondary_title"));
            citation.append(". ");
        }
        if (result.getString("volume") != null) {
            citation.append(result.getString("volume"));
        }
        if ((result.getString("volume") != null) && (result.getString("pages") != null)) {
            citation.append(":");
        }
        if (result.getString("pages") != null) {
            citation.append(result.getString("pages"));    
        }
        return citation.toString();
    }

    /**
     * Return first value of the given query parameter, decoded, or null if not present;
     */
    private String getFirstQueryValue(String parameter) {
        if (this.getQuery().getFirstValue(parameter) != null) {
            return Reference.decode(this.getQuery().getFirstValue(parameter));
        } else {
            return null;
        }
    }

}
