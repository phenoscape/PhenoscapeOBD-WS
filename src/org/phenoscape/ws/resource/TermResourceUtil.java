package org.phenoscape.ws.resource;

import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.phenoscape.obd.model.LinkedTerm;
import org.phenoscape.obd.model.Relationship;
import org.phenoscape.obd.model.Synonym;
import org.phenoscape.obd.model.Term;

public class TermResourceUtil {

    public static JSONObject translate(LinkedTerm term) throws JSONException {
        final JSONObject json = translateMinimal(term);
        json.put("parents", translateRelationships(term.getSubjectLinks()));
        json.put("children", translateRelationships(term.getObjectLinks()));
        final JSONArray synonyms = new JSONArray();
        for (Synonym synonym : term.getSynonyms()) {
            final JSONObject synonymObj = new JSONObject();
            synonymObj.put("name", synonym.getLabel());
            synonymObj.put("lang", synonym.getLanguage());
            synonymObj.put("scope", synonym.getScope());
            if (synonym.getType() != null) {
                synonymObj.put("type", translateMinimal(synonym.getType()));
            }
            synonyms.put(synonymObj);
        }
        json.put("synonyms", synonyms);
        json.put("definition", term.getDefinition());
        json.put("comment", term.getComment());
        final JSONObject source = new JSONObject();
        source.put("id", term.getSourceUID());
        json.put("source", source);
        return json;
    }

    public static JSONArray translateRelationships(Set<Relationship> relationships) throws JSONException {
        final JSONArray links = new JSONArray();
        for (Relationship relationship : relationships) {
            final JSONObject link = new JSONObject();
            link.put("target", translateMinimal(relationship.getOther()));
            link.put("relation", translateMinimal(relationship.getPredicate()));
            links.put(link);
        }
        return links;
    }

    public static JSONObject translateMinimal(Term term) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("id", term.getUID());
        json.put("name", term.getLabel());
        return json;
    }
    
}
