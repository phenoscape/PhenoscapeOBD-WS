package org.phenoscape.util;

public class PhenotypeAndAnnotatedSubtaxonCountDTO extends NodeDTO{
	
	private int phenotypeCount, subtaxonCount;

	public int getPhenotypeCount() {
		return phenotypeCount;
	}

	public void setPhenotypeCount(int phenotypeCount) {
		this.phenotypeCount = phenotypeCount;
	}

	public int getSubtaxonCount() {
		return subtaxonCount;
	}

	public void setSubtaxonCount(int subtaxonCount) {
		this.subtaxonCount = subtaxonCount;
	}

	public PhenotypeAndAnnotatedSubtaxonCountDTO(String id, String name){
		super(id);
		this.setName(name);
		phenotypeCount = 0;
		subtaxonCount = 0;
	}
}
