package org.phenoscape.obd.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.biojava.bio.seq.io.ParseException;
import org.biojavax.bio.phylo.io.nexus.NexusFile;
import org.json.JSONException;
import org.json.JSONObject;
import org.nexml.model.Document;
import org.nexml.model.DocumentFactory;

import com.eekboom.utils.Strings;

public class MatrixUtil {
    
    public static NexusFile translateToNEXUS(Matrix matrix) throws ParseException {
        //TODO
        final NexusFile nexus = new NexusFile();
//        final TaxaBlock taxaBlock = new TaxaBlock();
//        final AnnotationsQueryConfig config = new AnnotationsQueryConfig();
//        config.addPublicationID(pubID);
//        config.setSortColumn(SORT_COLUMN.TAXON);
//        final List<TaxonTerm> taxa = this.getAnnotatedTaxa(config);
//        for (TaxonTerm taxon : taxa) {
//            taxaBlock.addTaxLabel(taxon.getLabel());
//        }
//        taxaBlock.setDimensionsNTax(taxa.size());
//        nexus.addObject(taxaBlock);
        //TODO
        return nexus;
    }
    
    public static Document translateToNeXML(Matrix matrix) throws ParserConfigurationException {
        final Document nexml = DocumentFactory.createDocument();
        //TODO
        return nexml;
    }
    
    public static JSONObject translateToJSON(Matrix matrix) throws JSONException {
        final JSONObject json = new JSONObject();
        final List<Character> characters = matrix.getCharacters();
        Collections.sort(characters, new Comparator<Character>() {
            @Override
            public int compare(Character o1, Character o2) {
                return Strings.compareNatural(o1.getNumber(), o2.getNumber());
            }});
        final List<OTU> otus = matrix.getOTUs();
        Collections.sort(otus, new Comparator<OTU>() {
            @Override
            public int compare(OTU o1, OTU o2) {
                return Strings.compareNatural(o1.getLabel(), o2.getLabel());
            }
        });
        json.put("characters", translateCharacters(characters));
        json.put("otus", translateOTUs(otus));
        final JSONObject jsonMatrix = new JSONObject();
        for (OTU otu : otus) {
            final JSONObject otuStates = new JSONObject();
            jsonMatrix.put(otu.getUID(), otuStates);
            for (Character character : characters) {
                if (matrix.getState(otu, character) != null) {
                    otuStates.put(character.getUID(), translateState(matrix.getState(otu, character)));    
                }
            }
        }
        json.put("matrix", jsonMatrix);
        return json;
    }
    
    private static List<JSONObject> translateCharacters(List<Character> characters) throws JSONException {
        final List<JSONObject> jsonCharacters = new ArrayList<JSONObject>();
        for (Character character : characters) {
            final JSONObject json = new JSONObject();
            json.put("label", character.getLabel());
            json.put("id", character.getUID());
            json.put("num", character.getNumber());
            jsonCharacters.add(json);
        }
        return jsonCharacters;
    }
    
    private static List<JSONObject> translateOTUs(List<OTU> otus) throws JSONException {
        final List<JSONObject> jsonOTUs = new ArrayList<JSONObject>();
        for (OTU otu : otus) {
            final JSONObject json = new JSONObject();
            json.put("label", otu.getLabel());
            json.put("id", otu.getUID());
            json.put("comment", otu.getComment());
            final JSONObject taxon = new JSONObject();
            taxon.put("id", otu.getTaxon().getUID());
            taxon.put("label", otu.getTaxon().getLabel());
            taxon.put("extinct", otu.getTaxon().isExtinct());
            if (otu.getTaxon().getRank() != null) {
                final JSONObject rank = new JSONObject();
                rank.put("id", otu.getTaxon().getRank().getUID());
                rank.put("label", otu.getTaxon().getRank().getLabel());
                taxon.put("rank", rank);
            }
            
            json.put("taxon", taxon);
            jsonOTUs.add(json);
        }
        return jsonOTUs;
    }
    
    private static JSONObject translateState(Term state) throws JSONException {
        final JSONObject json = new JSONObject();
        json.put("label", state.getLabel());
        return json;
    }

}
