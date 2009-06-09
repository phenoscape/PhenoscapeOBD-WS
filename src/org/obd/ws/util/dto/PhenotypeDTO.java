package org.obd.ws.util.dto;

/**
 * @PURPOSE This is a Data Transfer Object to be used for
 * ferrying the phenotype data retrieved by SQL queries to the 
 * REST services in a defacto persistence layer
 * @author cartik
 *
 */
public class PhenotypeDTO {

	private String phenotypeId;
	/*
	 * These variables are for storing the values of the 
	 * columns from a standard phenotype query
	 */
	private String taxonId;
	private String taxon;

	private String entityId;
	private String entity;
	
	private String qualityId;
	private String quality;
	
	private String characterId;
	private String character;
	
	private String reifId;
	
	/*
	 * GETTERs and SETTERs
	 */
	
	public String getTaxonId() {
		return taxonId;
	}

	public void setTaxonId(String taxonId) {
		this.taxonId = taxonId;
	}

	public String getTaxon() {
		return taxon;
	}

	public void setTaxon(String taxon) {
		this.taxon = taxon;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public String getQualityId() {
		return qualityId;
	}

	public void setQualityId(String qualityId) {
		this.qualityId = qualityId;
	}

	public String getQuality() {
		return quality;
	}

	public void setQuality(String quality) {
		this.quality = quality;
	}

	public String getCharacterId() {
		return characterId;
	}

	public void setCharacterId(String characterId) {
		this.characterId = characterId;
	}

	public String getCharacter() {
		return character;
	}

	public void setCharacter(String character) {
		this.character = character;
	}

	public String getReifId() {
		return reifId;
	}

	public void setReifId(String reifId) {
		this.reifId = reifId;
	}
	
	// Only a GETTER for PhenotypeId
	public String getPhenotypeId(){
		return phenotypeId;
	}
	
	/**
	 * Constructor simply assigns the argument to the ID
	 * @param phenotypeId
	 */
	public PhenotypeDTO(String phenotypeId){
		this.phenotypeId = phenotypeId;
	}
}
