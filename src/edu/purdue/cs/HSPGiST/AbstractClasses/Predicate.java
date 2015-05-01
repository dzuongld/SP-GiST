package edu.purdue.cs.HSPGiST.AbstractClasses;

import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;
import edu.purdue.cs.HSPGiST.SupportClasses.Sized;

/**
 * Predicate superclass to enforce predicate
 * methods
 * @author Stefan Brinton
 *
 */
public abstract class Predicate implements WritableComparable<Predicate>, Copyable<Predicate>, Sized {

	/**
	 * Converts this predicate into a JSON-like object
	 * @return A unique string that can be parsed back into this Predicate
	 * <br>
	 * Please enclose your object in []. Otherwise unexpected behavior may occur
	 */
	public abstract String convertToJSON();
	
	/**
	 * Converts the string returned by convertToJSON to the fields
	 * of this predicate
	 */
	public abstract void convertFromJSON(String json);

}
