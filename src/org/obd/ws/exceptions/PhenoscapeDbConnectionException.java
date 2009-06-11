package org.obd.ws.exceptions;

/**
 * An exception that is thrown when neither database is available
 * for the services
 * @author cartik
 *
 */
public class PhenoscapeDbConnectionException extends Exception {

	private static final long serialVersionUID = -932236660167933510L;

	public PhenoscapeDbConnectionException(String msg){
		super(msg);
	}
}
