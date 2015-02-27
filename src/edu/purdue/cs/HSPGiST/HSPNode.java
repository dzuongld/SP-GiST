package edu.purdue.cs.HSPGiST;
/**
 * Gives nodes a shared class superclass for ease of use
 * @author Stefan Brinton
 *
 * @param <T> Node predicate type
 * @param <K> Node key type
 */
public abstract class HSPNode<T,K>{
	private HSPNode<T,K> parent;
	private T predicate;
	/**
	 * Predicate setter method
	 * @param predicate Value to set predicate to
	 */
	public void setPredicate(T predicate){
		this.predicate = predicate;
	}
	/**
	 * Predicate getter method
	 * @return Current value of predicate
	 */
	public T getPredicate(){
		return predicate;
	}
	
	public void setParent(HSPNode<T,K> parent){
		this.parent = parent;
	}
	public HSPNode<T,K> getParent(){
		return parent;
	}
}
