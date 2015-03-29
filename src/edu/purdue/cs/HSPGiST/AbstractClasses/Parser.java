package edu.purdue.cs.HSPGiST.AbstractClasses;

import java.util.ArrayList;

import edu.purdue.cs.HSPGiST.SupportClasses.Pair;

/**
 * Provides an abstract class to be the superclass
 * of any number of Parsers necessary for data
 * to be initially parsed by a mapper during index construction
 * Implementations of this class should implement one of the parse
 * methods not both (obviously)
 * 
 * @author Stefan Brinton & Daniel Fortney
 * 
 * @param <KeyIn> Type of Key from file
 * @param <ValueIn> Type of value from file
 * @param <KeyOut> Output's Key type
 * @param <ValueOut> Output's value type
 */
public abstract class Parser<KeyIn,ValueIn,KeyOut,ValueOut> {
	public boolean isArrayParse = false;
	//These two variables allow the local index constructor to know them for setting the output classes
	@SuppressWarnings("rawtypes")
	public Class keyout;
	@SuppressWarnings("rawtypes")
	public Class valout;
	/**
	 * Used to parse key-value pairs that generate a single output key-value pair
	 * @param key Input key
	 * @param value Input Value
	 * @return A pair containing the output key-value pair
	 */
	public abstract Pair<KeyOut, ValueOut> parse(KeyIn key, ValueIn value);
	
	/**
	 * Used to parse key-value pairs that generate multiple output pairs
	 * @param key
	 * @param value
	 * @return
	 */
	public abstract ArrayList<Pair<KeyOut,ValueOut>> arrayParse(KeyIn key, ValueIn value);
	

	/**
	 * Need to be able to make a copy for each mapper
	 */
	public abstract Parser<KeyIn, ValueIn, KeyOut, ValueOut> clone();
}
