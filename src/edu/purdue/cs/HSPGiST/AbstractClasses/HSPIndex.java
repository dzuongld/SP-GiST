package edu.purdue.cs.HSPGiST.AbstractClasses;

import java.util.ArrayList;

import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
/**
 * All indexes made with HSP-GiST will implement this
 * class and its methods. 
 * 
 * @author Stefan Brinton & Daniel Fortney
 *
 * @param <T> The type of the node predicates
 * @param <K> The type of the node keys
 * @param <R> The type of the node records
 */
public abstract class HSPIndex<T, K, R> {
	public int numSpaceParts;
	//TODO: figure out a reasonable value for this
	public static final int blocksize = 10;
	
	/**
	 * Maximum Number of decompositions allowed
	 */
	public int resolution;
	
	public ArrayList<Pair<T,Integer>> partitionPreds = null;
	public ArrayList<K> samples = null;
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
	public abstract boolean consistent(HSPNode<T,K,R> e, K q, int level);
	
	/**
	 * Check if the given key is consistent with a given predicate 
	 * (the value belongs to that node or its children)
	 * @param e The predicate to check for consistency with
	 * @param q The key to check for consistency
	 * @param level The depth within the tree (root is considered depth 1)
	 * @return True if the key is consistent with the node's predicate
	 */
	public abstract boolean consistent(T e, K q, int level);
	/**
	 * Governs splitting of an overfull leaf into numSpaceParts leaves
	 * also governs nodeshrink == false && pathshrink == NEVER trees creation of index nodes
	 * with correct predicates (if you have a tree with those values you will never split
	 * overfull nodes because they can't exist so this method is fundamentally different) 
	 * @param leaf The overfull leaf
	 * @param level The depth into the tree of the overfull leaf (root is depth 1)
	 * @param childrenData This is a return container, the data (keys and records) should
	 * be partitioned into the numSpaceParts ArrayLists inside this ArrayList 
	 * (This ArrayList of ArrayLists will be initialized; you shouldn't initialize it)
	 * @param childrenPredicates This is a return container, the predicates for each
	 * child node should be stored here
	 * <br>Note: ArrayList k and Predicate k, where 0<=k<numSpaceParts, are assumed to belong to the same child
	 * no checks will be performed for Keys and Predicates consistency
	 * @return True if a child will remain overfull and further splitting is needed
	 * false if no further splitting is needed
	 */
	public abstract boolean picksplit(HSPLeafNode<T,K,R> leaf, int level, ArrayList<ArrayList<Pair<K,R>>> childrenData, ArrayList<T> childrenPredicates);
	
	/**
	 * This method should use the ArrayList<K> samples and should set the ArrayList<Pair<T,Integer>> partitionPreds
	 * Use samples to approximate how to divide input data to balance load across reducers
	 * When this method completes partitionPreds should contain numOfReducers (read as a number, e.g. 32) mutually exclusive predicates 
	 * (no predicate is a part of another predicate's subtree) with the depth in the tree each predicate is at
	 * In short, find mutually exclusive predicates such that an equal number of samples are in each predicate
	 * <br>
	 * Note: The predicates should cover the spatial domain, e.g. four rectangles that represent the quadrants of a 2D Cartesian plot   
	 * @param numOfReducers the number of Predicate , depth pairs needed
	 */
	public abstract void setupPartitions(int numOfReducers);
	
	/**
	 * Partitions keys into separate reducers
	 * @param key The key being partitioned
	 * @param record The keys associated record
	 * @param numOfReducers The number of reducers
	 * @return The partition the key has been placed in
	 */
	public int partition(K key, R record, int numOfReducers){
		for(int i = 0; i < numOfReducers;i++){
			if(consistent(partitionPreds.get(i).getFirst(), key, partitionPreds.get(i).getSecond()))
				return i;
		}
		/*
		 * This should never happen
		 */
		return 0;
	}
	
	/**
	 * Trees with NodeShrink == true									
	 * Require an additional method to provide nodes with
	 * a predicate when they are made. All other trees can implement a
	 * trivial variant (return null)
	 * @param key The key requiring a new IndexNode to be made
	 * @param parentPred The parent's predicate
	 * @param level The new node's level (check if level == 2 to properly handle root's children)
	 * @return The predicate for the new node
	 */
	public abstract T determinePredicate(K key, T parentPred, int level);
	
	/**
	 * This method handles insertion of keys and records into an index
	 * @param root The root of the index being constructed
	 * @param key The key being inserted into the index
	 * @param record The record attached to the key being inserted
	 * @param level The depth the key is attempting to be inserted at
	 * @return The root node of the current subtree
	 */
	public HSPNode<T,K,R> insert(HSPNode<T,K,R> root, K key, R record, int level){
		HSPNode<T,K,R> curr = root;
		if(path == PathShrink.NEVER){
			for(; level < resolution; level++){
				if(curr == null){
					root = new HSPIndexNode<T, K,R>(null,this,level);
					curr = root;
				}
				int index = -1;
				HSPIndexNode<T,K,R> ind =((HSPIndexNode<T,K,R>)curr); 
				for(int i=0; i < ind.children.size(); i++){
					if(consistent(ind.children.get(i), key, level+1)){
						index = i;
						break;
					}
				}
				if(index == -1){
					ind.children.add(new HSPIndexNode<T,K,R>(determinePredicate(key, ind.getPredicate(),level+1), ind));
					index = ind.children.size()-1;
				}
				else{
					//we got the next node but we need to check if it is a leaf and convert it to an
					//index node if it is :: This only happens if PathShrink == NEVER && nodeshrink == false
					if(ind.children.get(index) instanceof HSPLeafNode<?,?,?>){
						HSPIndexNode<T,K,R> replace = new HSPIndexNode<T,K,R>(ind.children.get(index).getPredicate(), ind, this, level+1);
						ind.children.set(index, replace);
					}
				}
				curr = ind.children.get(index);
			}
		}
		if(curr == null){
			//We have just started tree construction
			root = new HSPLeafNode<T,K,R>(null);
			curr = root;
		}
		if(curr instanceof HSPIndexNode<?,?,?>){
			//TODO: The implementation of tree shrink should be right here
			//But I have the nariest a clue as to how to implement it
			HSPIndexNode<T,K,R> ind =((HSPIndexNode<T,K,R>)curr);
			int index = -1;
			for(int i = 0; i < ind.children.size(); i++){
				if(consistent(ind.children.get(i), key, level)){
					index = i;
					break;
				}
			}
			if(index == -1){
				ind.children.add(new HSPLeafNode<T,K,R>(ind, determinePredicate(key, ind.getPredicate(),level+1)));
				index = ind.children.size()-1;
			}
			insert(ind.children.get(index), key, record, level+1);
			return root;
		}
		//If we get here we are a leaf
		HSPLeafNode<T,K,R> leaf =((HSPLeafNode<T,K,R>)curr);
		HSPIndexNode<T,K,R> retVal = null;
		if(leaf.keys.size() == blocksize){
			//Actually overfill the leaf and then split it 
			leaf.keys.add(new Pair<K,R>(key,record));
			boolean overfull;
			while(true){
				ArrayList<ArrayList<Pair<K,R>>> keysets = new ArrayList<ArrayList<Pair<K,R>>>();
				for(int i = 0; i < numSpaceParts; i++){
					keysets.add(new ArrayList<Pair<K,R>>());
				}
				ArrayList<T> preds = new ArrayList<T>();
				HSPIndexNode<T,K,R> replace;
				overfull = picksplit(leaf, level, keysets, preds);
				replace = new HSPIndexNode<T,K,R>(leaf.getPredicate(), leaf.getParent());
				for(int i = 0; i < keysets.size(); i++){
					if(keysets.get(i).size() != 0 || nodeShrink == false){
						replace.children.add(new HSPLeafNode<T,K,R>(replace, keysets.get(i), preds.get(i)));
					}
				}
				if(leaf.getParent()!=null){
					int index = ((HSPIndexNode<T,K,R>) leaf.getParent()).children.indexOf(leaf);
					//Replace the leaf version with the index version in its parent
					((HSPIndexNode<T,K,R>) leaf.getParent()).children.set(index, replace);
				}
				level++;
				if(retVal == null)
					retVal = replace;
				if(overfull){
					//only one child can be overfull on a decomposition
					for(int i = 0; i < replace.children.size();i++)
						if(((HSPLeafNode<T,K,R>)replace.children.get(i)).keys.size() > numSpaceParts){
							leaf = ((HSPLeafNode<T,K,R>)replace.children.get(i));
						}
				}
				else{
					
					return retVal;
				}
			}
		}
		else{
			leaf.keys.add(new Pair<K,R>(key,record));
			return leaf;
		}
	}
}
