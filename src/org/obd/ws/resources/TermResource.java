package org.obd.ws.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.obd.model.Node;
import org.phenoscape.obd.query.OBDQuery;
import org.phenoscape.util.NodeDTO;
import org.phenoscape.util.Queries;
import org.phenoscape.util.TTOTaxonomy;
import org.phenoscape.ws.application.PhenoscapeWebServiceApplication;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;

public class TermResource extends AbstractOBDResource {

    private String termId;
    private JSONObject jObjs;	
    private Queries queries;
    private Connection conn;
    private OBDQuery obdq; 
    private TTOTaxonomy ttoTaxonomy; 

    @Override
    protected void doInit() throws ResourceException {
        super.doInit();
        this.queries = (Queries)this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.QUERIES_STRING);
        this.termId = Reference.decode((String) (this.getRequestAttributes().get("termID")));
        this.ttoTaxonomy = (TTOTaxonomy)this.getContext().getAttributes().get(PhenoscapeWebServiceApplication.TTO_TAXONOMY_STRING);
    }

    /**
     * This is the most important method in this class, overridden. 
     * It calls the method which finds all the information that is 
     * pertinent to the input term and throws exceptions and sets 
     * error statuses if the input term is invalid 
     */
    @Get("json")
    public Representation getJSONRepresentation() throws ResourceException {
        try {
            this.connectShardToDatabase();
            this.conn = this.getShard().getConnection();
            this.obdq = new OBDQuery(this.getShard());
            this.jObjs = getTermInfo(this.termId);
            if (this.jObjs == null) {
                getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND,
                "The search term was not found");
                disconnectShardFromDatabase();
                return null;
            }
        } catch (JSONException e) {
            log().fatal(e);
            getResponse().setStatus(Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY,
            "JSON EXCEPTION");
            return null;
        } catch(SQLException e){
            log().fatal(e);
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "SQL Exception");
            return null;
        } catch (ClassNotFoundException e) {
            log().fatal(e);
            getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Class not found Exception");
            return null;
        } finally{
            disconnectShardFromDatabase();
        }
        return new JsonRepresentation(this.jObjs);
    }

    /**
     * This method invokes the OBDQuery class and its methods
     * for finding term information. Returned results from the
     * OBDQuery methods are
     * packaged into the JSON Object and returned    
     * @param termId - search term
     * @return JSON Object
     * @throws JSONException
     * @throws SQLException
     */
    private JSONObject getTermInfo(String termId) throws JSONException, SQLException {

        JSONObject jsonObj = new JSONObject();

        Set<JSONObject> parents = new HashSet<JSONObject>();
        Set<JSONObject> children = new HashSet<JSONObject>();
        Set<JSONObject> synonyms = new HashSet<JSONObject>();

        String relationId, relationName, targetId, targetName, subjectId, subjectName;
        String rankId, rank;
        String comment = "", definition = "";
        String synonym;

        if (this.getShard().getNode(termId) != null) {
            jsonObj.put("id", termId);
            Node termNode = this.getShard().getNode(termId);
            String termLabel = termNode.getLabel();
            if(termLabel == null)
                termLabel = obdq.simpleLabel(termId);
            jsonObj.put("name", termLabel);

            if(termId.startsWith("TTO")){
                NodeDTO taxonDTO = new NodeDTO(termId);
                taxonDTO.setName(termLabel);
                if(ttoTaxonomy.getSetOfExtinctTaxa().contains(taxonDTO))
                    jsonObj.put("extinct", true);
                else
                    jsonObj.put("extinct", false);
            }

            PreparedStatement parentOfTermStmt = conn.prepareStatement(queries.getParentOfTermQuery());
            parentOfTermStmt.setString(1, termId);
            ResultSet rsForParentQuery = parentOfTermStmt.executeQuery();
            while(rsForParentQuery.next()){
                relationId = rsForParentQuery.getString(3);
                relationName = rsForParentQuery.getString(4);
                targetId = rsForParentQuery.getString(5);
                targetName = rsForParentQuery.getString(6);
                rankId = rsForParentQuery.getString(7);
                rank = rsForParentQuery.getString(8);
                if(!relationId.contains("DbXref")){ //avoid DbXrefs
                    JSONObject parent = new JSONObject();
                    JSONObject relation = new JSONObject();
                    JSONObject target = new JSONObject();
                    relation.put("id", relationId);
                    relation.put("name", relationName);
                    target.put("id", targetId);
                    target.put("name", targetName);
                    parent.put("relation", relation);
                    parent.put("target", target);
                    if(rankId != null){
                        JSONObject rankObj = new JSONObject();
                        rankObj.put("id", rankId);
                        rankObj.put("name", rank);
                        target.put("rank", rankObj);
                        rankId = null;
                        rank = null;

                        NodeDTO tdto = new NodeDTO(targetId);
                        tdto.setName(targetName);
                        if(ttoTaxonomy.getSetOfExtinctTaxa().contains(tdto))
                            target.put("extinct", true);
                        else
                            target.put("extinct", false);

                    }
                    parents.add(parent);
                }
            }

            PreparedStatement childrenOfTermStmt = conn.prepareStatement(queries.getChildrenOfTermQuery());
            childrenOfTermStmt.setString(1, termId);
            ResultSet rsForChildrenQuery = childrenOfTermStmt.executeQuery();
            while(rsForChildrenQuery.next()){
                subjectId = rsForChildrenQuery.getString(1);
                subjectName = rsForChildrenQuery.getString(2);
                relationId = rsForChildrenQuery.getString(3);
                relationName = rsForChildrenQuery.getString(4);
                rankId = rsForChildrenQuery.getString("rank_uid");
                rank = rsForChildrenQuery.getString("rank_label");
                if(!relationId.contains("DbXref")){ //avoid DBXrefs
                    JSONObject child = new JSONObject();
                    JSONObject relation = new JSONObject();
                    JSONObject target = new JSONObject();
                    relation.put("id", relationId);
                    relation.put("name", relationName);
                    target.put("id", subjectId);
                    target.put("name", subjectName);
                    child.put("relation", relation);
                    child.put("target", target);
                    if(rankId != null){
                        JSONObject rankObj = new JSONObject();
                        rankObj.put("id", rankId);
                        rankObj.put("name", rank);
                        target.put("rank", rankObj);
                        rankId = null;
                        rank = null;

                        NodeDTO tdto = new NodeDTO(subjectId);
                        tdto.setName(subjectName);
                        if(ttoTaxonomy.getSetOfExtinctTaxa().contains(tdto))
                            target.put("extinct", true);
                        else
                            target.put("extinct", false);
                    }
                    children.add(child);
                }
            }
            jsonObj.put("parents", parents);
            jsonObj.put("children", children);

            PreparedStatement commentForTermStmt = conn.prepareStatement(queries.getCommentOnTermQuery());
            commentForTermStmt.setString(1, termId);
            ResultSet rsForCommentQuery = commentForTermStmt.executeQuery();
            while(rsForCommentQuery.next()){
                comment = rsForCommentQuery.getString(1);
            }
            jsonObj.put("comment", comment);

            PreparedStatement synonymOfTermStmt = conn.prepareStatement(queries.getSynonymOfTermQuery());
            synonymOfTermStmt.setString(1, termId);
            ResultSet rsForSynonymQuery = synonymOfTermStmt.executeQuery();
            while(rsForSynonymQuery.next()){
                synonym = rsForSynonymQuery.getString(1);
                JSONObject synonymObj = new JSONObject();
                synonymObj.put("name", synonym);
                synonyms.add(synonymObj);
            }
            jsonObj.put("synonyms", synonyms);

            PreparedStatement definitionOfTermStmt = conn.prepareStatement(queries.getDefinitionOfTermQuery());
            definitionOfTermStmt.setString(1, termId);
            ResultSet rsForDefinitionQuery = definitionOfTermStmt.executeQuery();
            while(rsForDefinitionQuery.next()){
                definition = rsForDefinitionQuery.getString(1);
            }
            if (definition.length() > 0)
                jsonObj.put("definition", definition);

        } else {
            jsonObj = null;
        }
        return jsonObj;
    }

}
