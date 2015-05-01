package edu.purdue.cs.HSPGiST.AbstractClasses;

import edu.purdue.cs.HSPGiST.SupportClasses.Copyable;

/**
 * Gives nodes a shared class superclass for ease of use
 * 
 * @author Stefan Brinton
 *
 * @param <K>
 *            Node key type
 * @param <R>
 *            Node record type
 */
public abstract class HSPNode<K, R> implements Copyable<HSPNode<K, R>> {
	protected HSPNode<K, R> parent;
	protected Predicate predicate;
	/**
	 * The size of the node's remaining data following the predicate + the size
	 * of the node's subtree (this only really applies to indexNodes)
	 */
	public long size = 0;

	/**
	 * This value represents the number of predicates merged into this node
	 * Used to account for TreeShrink and the root nodes of local trees
	 * not being at a perceived depth
	 */
	private int offset = 0;

	/**
	 * Predicate setter method
	 * 
	 * @param predicate
	 *            Value to set predicate to
	 */
	public void setPredicate(Predicate predicate) {
		this.predicate = predicate;
	}

	/**
	 * Predicate getter method
	 * 
	 * @return Current value of predicate
	 */
	public Predicate getPredicate() {
		return predicate;
	}

	public void setParent(HSPNode<K, R> parent) {
		this.parent = parent;
	}

	public HSPNode<K, R> getParent() {
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
	 * @param offset
	 *            the offset to set
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}
}
