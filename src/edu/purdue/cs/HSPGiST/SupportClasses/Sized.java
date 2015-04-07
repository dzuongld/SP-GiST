package edu.purdue.cs.HSPGiST.SupportClasses;

/**
 * This interface is for getting the size of an object's output when it is written
 * using its write method in bytes
 * @author Stefan Brinton
 *
 */
public interface Sized {
	
	/**
	 * @return The number of bytes written by this method when write is called
	 */
	public long getSize();
}
