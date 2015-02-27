package edu.purdue.cs.HSPGiST;

import java.util.ArrayList;
/**
 * All indexes made with HSP-GiST will implement this
 * class and its methods. 
 * 
 * @author Stefan Brinton & Daniel Fortney
 *
 * @param <T> The type of the node predicates
 * @param <K> The type of the node keys
 */
public abstract class HSPIndex<T, K> {
	public int numSpaceParts;
	//This is a dummy value for now
	public static final int blocksize = 4;
	
	/**
	 * Maximum Number of decompositions allowed
	 */
	public int resolution;
	
	/**
	 * PathShrink Enum
	 * NEVER - A tree will insert a value at the greatest depth possible
	 * LEAF - A tree will insert a value to the first available leaf; 
	 * splitting won't be done until a leaf overflows its bucket
	 * TREE - A tree will merge index nodes from a LEAF tree with a single child with their child
	 * until no index node has a single child
	 */
	public static enum PathShrink {NEVER, LEAF, TREE};
	public PathShrink path;
	
	/**
	 * If true, no empty partitions (empty leaf or index nodes) will be present in the index
	 */
	public boolean nodeShrink;
	
	/**
	 * Check if the given key is consistent with the given node's predicate 
	 * (the value belongs to that node or its children)
	 * @param e The node to check for consistency with
	 * @param q The key to check for consistency
	 * @param level The depth within the tree (root is considered depth 1)
	 * @return True if the key is consistent with the node's predicate
	 */
	public abstract boolean consistent(HSPNode<T,K> e, K q, int level);
	
	/**
	 * Governs splitting of an overfull leaf into numSpaceParts leaves
	 * also governs nodeshrink == false && pathshrink == NEVER trees creation of index nodes
	 * with correct predicates (if you have a tree with those values you will never split
	 * overfull nodes because they can't exist so this method is fundamentally different) 
	 * @param leaf The keys inside the overfull leaf
	 * @param level The depth into the tree of the overfull tree (root is depth 1)
	 * @param childrenKeys This is a return container, the keys
	 * should be partitioned into numSpaceParts ArrayLists these ArrayLists will be
	 * the key lists for the newly made leaves including empty leaves
	 * (This ArrayList of ArrayLists will be initialized on both levels with the correct number of ArrayLists)
	 * @param childrenPredicates This is a return container, the predicates for each
	 * child node should be stored here
	 * <br>Note: ArrayList 0 and Predicate 0 are assumed to belong to the same child
	 * no checks will be performed for Keys and Predicates consistency
	 * @return True if a child will remain overfull or further splitting is needed
	 * false if no further splitting is needed
	 */
	public abstract boolean picksplit(HSPLeafNode<T,K> leaf, int level, ArrayList<ArrayList<K>> childrenKeys, ArrayList<T> childrenPredicates);
	
	/**
	 * Trees with NEVER pathshrink and NodeShrink == true
	 * Require an additional method to provide nodes with
	 * a predicate when they are made. All other trees can implement a
	 * trivial variant (return null)
	 * @param key The key requiring a new IndexNode to be made
	 * @param parentPred The parent's predicate
	 * @param level The new node's level (check if level == 2 to properly handle root's children)
	 * @return The predicate for the new node
	 */
	public abstract T determinePredicate(K key, T parentPred, int level);
	
	public HSPNode<T,K> insert(HSPNode<T,K> root, K key, int level){
		HSPNode<T,K> curr = root;
		if(path == PathShrink.NEVER){
			for(; level < resolution; level++){
				if(curr == null){
					root = new HSPIndexNode<T, K>(null,this,level);
					curr = root;
				}
				int index = -1;
				HSPIndexNode<T,K> ind =((HSPIndexNode<T,K>)curr); 
				for(int i=0; i < ind.children.size(); i++){
					if(consistent(ind.children.get(i), key, level+1)){
						index = i;
						break;
					}
				}
				if(index == -1){
					ind.children.add(new HSPIndexNode<T,K>(determinePredicate(key, ind.getPredicate(),level+1), ind));
					index = ind.children.size()-1;
				}
				else{
					//we got the next node but we need to check if it is a leaf and convert it to an
					//index node if it is :: This only happens if PathShrink == NEVER && nodeshrink == false
					if(ind.children.get(index) instanceof HSPLeafNode<?,?>){
						HSPIndexNode<T,K> replace = new HSPIndexNode<T,K>(ind.children.get(index).getPredicate(),ind.children.get(index).getParent(), this, level+1);
						ind.children.set(index, replace);
					}
				}
				curr = ind.children.get(index);
			}
		}
		if(curr == null){
			//We have just started tree construction
			root = new HSPLeafNode<T,K>(null);
			curr = root;
		}
		if(curr instanceof HSPIndexNode<?,?>){
			//TODO: The implementation of tree shrink should be right here
			//But I have the nariest a clue as to how to implement it
			HSPIndexNode<T,K> ind =((HSPIndexNode<T,K>)curr);
			int index = -1;
			for(int i = 0; i < ind.children.size(); i++){
				if(consistent(ind.children.get(i), key, level)){
					index = i;
				}
			}
			if(index == -1){
				ind.children.add(new HSPLeafNode<T,K>(ind, determinePredicate(key, ind.getPredicate(),level+1)));
				index = ind.children.size()-1;
			}
			insert(ind.children.get(index), key, level+1);
			return root;
		}
		//If we get here we are a leaf
		HSPLeafNode<T,K> leaf =((HSPLeafNode<T,K>)curr);
		if(leaf.keys.size() == blocksize){
			ArrayList<ArrayList<K>> keysets = new ArrayList<ArrayList<K>>();
			for(int i = 0; i < numSpaceParts; i++){
				keysets.add(new ArrayList<K>());
			}
			ArrayList<T> preds = new ArrayList<T>();
			HSPIndexNode<T,K> replace;
			boolean overfull = picksplit(leaf, level, keysets, preds);
			do{
				ArrayList<HSPNode<T,K>> children = new ArrayList<HSPNode<T,K>>();
				replace = new HSPIndexNode<T,K>(leaf.getPredicate(), leaf.getParent());
				if(leaf.getParent()!=null){
					int index = ((HSPIndexNode<T,K>) leaf.getParent()).children.indexOf(leaf);
					//Replace the leaf version with the index version in its parent
					((HSPIndexNode<T,K>) leaf.getParent()).children.set(index, replace);
				}
				for(int i = 0; i < keysets.size(); i++){
					if(keysets.get(i).size() != 0 || nodeShrink == false){
						children.add(new HSPLeafNode<T,K>(replace, keysets.get(i), preds.get(i)));
					}
				}
				
				replace.children = children;
				level++;
				if(overfull){
					//only one child can be overfull on a decomposition
					for(int i = 0; i < replace.children.size();i++)
						if(((HSPLeafNode<T,K>)replace.children.get(i)).keys.size() > numSpaceParts){
							leaf = ((HSPLeafNode<T,K>)replace.children.get(i));
						}
				}
			}while((overfull = picksplit(leaf, level, keysets, preds)));
			return replace;
		}
		else{
			leaf.keys.add(key);
			return leaf;
		}
	}
}
