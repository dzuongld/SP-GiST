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
	 * The size of this node's subtree excluding itself
	 * for index nodes<br>
	 * The size of the node's remaining data following the predicate for
	 * leaf and reference nodes
	 */
	protected long size = 0;

	private int offset = 0;
	
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

	/**
	 * 
	 * @return The size of this node's subtree including itself
	 */
	public abstract long getSize();

	/**
	 * @return the offset
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * @param offset the offset to set
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}
}
