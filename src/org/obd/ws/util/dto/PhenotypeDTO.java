package org.obd.ws.util.dto;

/**
 * This is a Data Transfer Object to be used for
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
	
	private String relatedEntityId;
	private String relatedEntity;
	
	private String reifId;
	
	private String numericalCount;
	
	private String measurement;
	private String unit; 
	
	/*
	 * GETTERs and SETTERs
	 */
	
	public String getNumericalCount() {
		return numericalCount;
	}
	public void setNumericalCount(String numericalCount) {
		this.numericalCount = numericalCount;
	}

	public String getMeasurement() {
		return measurement;
	}
	public void setMeasurement(String measurement) {
		this.measurement = measurement;
	}

	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}

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

	public String getRelatedEntityId() {
		return relatedEntityId;
	}
	public void setRelatedEntityId(String relatedEntityId) {
		this.relatedEntityId = relatedEntityId;
	}
	public String getRelatedEntity() {
		return relatedEntity;
	}
	public void setRelatedEntity(String relatedEntity) {
		this.relatedEntity = relatedEntity;
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
	
	/**
	 * A comparison method for this class. Two PhenotypeDTOs are
	 * equal only if the Taxon and the Phenotype are the same
	 * @param dto
	 * @return a boolean to indicate if the argument object is the same
	 * as this calling object (*this from C++)
	 */
	@Override
	public boolean equals(Object dto){
		if(this == dto) return true;
		if(!(dto instanceof PhenotypeDTO)) return false;
		PhenotypeDTO pdto = (PhenotypeDTO)dto;
		if(pdto.getTaxonId().equals(this.getTaxonId()) && 
				pdto.getPhenotypeId().equals(this.getPhenotypeId())){
			return true;
		}
		return false; 
	}
	
	/**
	 * Overriden hashcode method
	 * @return generated hashcode for the object
	 */
	@Override
	public int hashCode(){
		int hash = 7;
		hash = hash*(this.getTaxonId().hashCode() + this.getPhenotypeId().hashCode());
		return hash;
	}
}
