package edu.purdue.cs.HSPGiST;
/**
 * This is an interface similar to the Cloneable interface
 * Cloneable does not ensure the presence of the .clone() method
 * As such, we use Copyable to ensure the presence of the .copy() method
 * that way we can cast user data to Copyable so it may be cloned into the index
 * @author Stefan Brinton
 *
 */
public interface Copyable<T> {
	public T copy();
}
