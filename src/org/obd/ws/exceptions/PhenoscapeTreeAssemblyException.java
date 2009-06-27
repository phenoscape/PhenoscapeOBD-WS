package org.obd.ws.exceptions;
/**
 * An exception that is thrown when there is a problem
 * in generatng the tree from the input taxon to 
 * phenotype assertions
 * @author cartik
 *
 */

public class PhenoscapeTreeAssemblyException extends Exception{
	
	private static final long serialVersionUID = 7649731646724043305L;
	
	public PhenoscapeTreeAssemblyException(String msg){
		super(msg);
	}
}
