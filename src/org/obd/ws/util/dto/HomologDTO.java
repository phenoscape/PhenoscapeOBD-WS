package org.obd.ws.util.dto;

/**
 * @PURPOSE This is a Data Transfer Object to be used for
 * ferrying the homology data retrieved by SQL queries to the 
 * REST services in a defacto persistence layer
 * @author cartik
 *
 */

public class HomologDTO {

	private String homologId; 
	
	/*
	 * These variables are for storing the values of the 
	 * columns from a standard homolog query
	 */
	private String lhTaxonId;
	private String lhTaxon;
	
	private String rhTaxonId;
	private String rhTaxon;
	
	private String lhEntityId;
	private String lhEntity;

	private String rhEntityId;
	private String rhEntity;
	
	private String publication;
	
	private String evidenceCode;
	private String evidence;

	/*
	 * GETTERs and SETTERs
	 */
	
	public String getLhTaxonId() {
		return lhTaxonId;
	}
	public void setLhTaxonId(String lhTaxonId) {
		this.lhTaxonId = lhTaxonId;
	}
	public String getLhTaxon() {
		return lhTaxon;
	}
	public void setLhTaxon(String lhTaxon) {
		this.lhTaxon = lhTaxon;
	}
	public String getRhTaxonId() {
		return rhTaxonId;
	}
	public void setRhTaxonId(String rhTaxonId) {
		this.rhTaxonId = rhTaxonId;
	}
	public String getRhTaxon() {
		return rhTaxon;
	}
	public void setRhTaxon(String rhTaxon) {
		this.rhTaxon = rhTaxon;
	}
	public String getLhEntityId() {
		return lhEntityId;
	}
	public void setLhEntityId(String lhEntityId) {
		this.lhEntityId = lhEntityId;
	}
	public String getLhEntity() {
		return lhEntity;
	}
	public void setLhEntity(String lhEntity) {
		this.lhEntity = lhEntity;
	}
	public String getRhEntityId() {
		return rhEntityId;
	}
	public void setRhEntityId(String rhEntityId) {
		this.rhEntityId = rhEntityId;
	}
	public String getRhEntity() {
		return rhEntity;
	}
	public void setRhEntity(String rhEntity) {
		this.rhEntity = rhEntity;
	}
	public String getPublication() {
		return publication;
	}
	public void setPublication(String publication) {
		this.publication = publication;
	}
	public String getEvidenceCode() {
		return evidenceCode;
	}
	public void setEvidenceCode(String evidenceCode) {
		this.evidenceCode = evidenceCode;
	}
	public String getEvidence() {
		return evidence;
	}
	public void setEvidence(String evidence) {
		this.evidence = evidence;
	}
	
	//homolog ID is read only
	public String getHomologId() {
		return homologId;
	}
	
	/**
	 * Constructor
	 * @param homologId - this is the identifier for the homolog DTO. It is set arbitrarily
	 * by convention 
	 */
	public HomologDTO(String homologId){
		this.homologId = homologId; 
	}
	
}
