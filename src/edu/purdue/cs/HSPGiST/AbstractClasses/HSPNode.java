package edu.purdue.cs.HSPGiST.AbstractClasses;

import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;

/**
 * Gives nodes a shared class superclass for ease of use
 * 
 * @author Stefan Brinton
 *
 * @param <T>
 *            Node predicate type
 * @param <K>
 *            Node key type
 * @param <R>
 *            Node record type
 */
public abstract class HSPNode<T, K, R> implements Copyable<HSPNode<T, K, R>> {
	protected HSPNode<T, K, R> parent;
	protected T predicate;

	/**
	 * Predicate setter method
	 * 
	 * @param predicate
	 *            Value to set predicate to
	 */
	public void setPredicate(T predicate) {
		this.predicate = predicate;
	}

	/**
	 * Predicate getter method
	 * 
	 * @return Current value of predicate
	 */
	public T getPredicate() {
		return predicate;
	}

	public void setParent(HSPNode<T, K, R> parent) {
		this.parent = parent;
	}

	public HSPNode<T, K, R> getParent() {
		return parent;
	}
}
