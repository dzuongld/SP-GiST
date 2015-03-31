package edu.purdue.cs.HSPGiST.AbstractClasses;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import edu.purdue.cs.HSPGiST.SupportClasses.HSPIndexNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPReferenceNode;
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
	
	public ArrayList<K> samples = new ArrayList<K>();
	public HSPIndexNode<T,K,R> globalRoot = new HSPIndexNode<T,K,R>();
	private ArrayList<Pair<T,Integer>> partRoots = new ArrayList<Pair<T,Integer>>();
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
	 * @return True if the key is consistent with the predicate
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
	 * Additionally, while finding partitionPreds this method should construct the Global Index Tree with
	 * globalRoot
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
		HSPNode<T,K,R> curr = globalRoot;
		int dep = 1;
		while(true){
			for(HSPNode<T,K,R> child : ((HSPIndexNode<T,K,R>) curr).getChildren()){
				if(consistent(child, key, dep)){
					dep++;
					curr = child;
				}
				if(curr instanceof HSPReferenceNode<?,?,?>){
					return ((HSPReferenceNode<T,K,R>)child).getFileNumber();
				}
			}
			
		}
	}
	
	/**
	 * Used by LocalReducer to get its roots predicate and depth
	 * @param key The key being partitioned
	 * @param record The keys associated record
	 * @param numOfReducers The number of reducers
	 * @return The partition the key has been placed in
	 */
	public Pair<T,Integer> getPartition(int pNumber){
		return partRoots.get(pNumber);
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
				int index = -1;
				HSPIndexNode<T,K,R> ind =((HSPIndexNode<T,K,R>)curr); 
				for(int i=0; i < ind.getChildren().size(); i++){
					if(consistent(ind.getChildren().get(i), key, level+1)){
						index = i;
						break;
					}
				}
				if(index == -1){
					ind.getChildren().add(new HSPIndexNode<T,K,R>(ind, determinePredicate(key, ind.getPredicate(),level+1)));
					index = ind.getChildren().size()-1;
				}
				else{
					//we got the next node but we need to check if it is a leaf and convert it to an
					//index node if it is :: This only happens if PathShrink == NEVER && nodeshrink == false
					if(ind.getChildren().get(index) instanceof HSPLeafNode<?,?,?>){
						HSPIndexNode<T,K,R> replace = new HSPIndexNode<T,K,R>(ind, ind.getChildren().get(index).getPredicate(), this, level+1);
						ind.getChildren().set(index, replace);
					}
				}
				curr = ind.getChildren().get(index);
			}
		}
		if(curr instanceof HSPIndexNode<?,?,?>){
			//TODO: The implementation of tree shrink should be right here
			//But I have the nariest a clue as to how to implement it
			HSPIndexNode<T,K,R> ind =((HSPIndexNode<T,K,R>)curr);
			int index = -1;
			for(int i = 0; i < ind.getChildren().size(); i++){
				if(consistent(ind.getChildren().get(i), key, level)){
					index = i;
					break;
				}
			}
			if(index == -1){
				ind.getChildren().add(new HSPLeafNode<T,K,R>(ind, determinePredicate(key, ind.getPredicate(),level+1)));
				index = ind.getChildren().size()-1;
			}
			insert(ind.getChildren().get(index), key, record, level+1);
			return root;
		}
		//If we get here we are a leaf
		HSPLeafNode<T,K,R> leaf =((HSPLeafNode<T,K,R>)curr);
		HSPIndexNode<T,K,R> retVal = null;
		if(leaf.getKeyRecords().size() == blocksize && level < resolution){
			//Actually overfill the leaf and then split it 
			leaf.getKeyRecords().add(new Pair<K,R>(key,record));
			boolean overfull;
			while(true){
				ArrayList<ArrayList<Pair<K,R>>> keysets = new ArrayList<ArrayList<Pair<K,R>>>();
				for(int i = 0; i < numSpaceParts; i++){
					keysets.add(new ArrayList<Pair<K,R>>());
				}
				ArrayList<T> preds = new ArrayList<T>();
				HSPIndexNode<T,K,R> replace;
				overfull = picksplit(leaf, level, keysets, preds);
				replace = new HSPIndexNode<T,K,R>(leaf.getParent(), leaf.getPredicate());
				for(int i = 0; i < keysets.size(); i++){
					if(keysets.get(i).size() != 0 || nodeShrink == false){
						replace.getChildren().add(new HSPLeafNode<T,K,R>(replace, preds.get(i), keysets.get(i)));
					}
				}
				if(leaf.getParent()!=null){
					int index = ((HSPIndexNode<T,K,R>) leaf.getParent()).getChildren().indexOf(leaf);
					//Replace the leaf version with the index version in its parent
					((HSPIndexNode<T,K,R>) leaf.getParent()).getChildren().set(index, replace);
				}
				level++;
				if(retVal == null)
					retVal = replace;
				if(overfull){
					//only one child can be overfull on a decomposition
					for(int i = 0; i < replace.getChildren().size();i++)
						if(((HSPLeafNode<T,K,R>)replace.getChildren().get(i)).getKeyRecords().size() > numSpaceParts){
							leaf = ((HSPLeafNode<T,K,R>)replace.getChildren().get(i));
						}
				}
				else{
					
					return retVal;
				}
			}
		}
		else if(leaf.getKeyRecords().size() < blocksize){
			leaf.getKeyRecords().add(new Pair<K,R>(key,record));
			return leaf;
		}
		return root;
	}

	/*
	 * The following methods are for use in setupPartitions and no where else
	 */
	
	/**
	 * Setups of the global root with the given predicates for its children
	 * @param preds The predicates for the global root's children
	 * @return An arraylist of pairs containing the global root's children with the keys from
	 * samples in the keyRecords of the node they are consistent with and the depth of each node (2)
	 */
	protected ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>> initializeGlobalRoot(ArrayList<T> preds){
		ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>> lowNodes = new ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>>();
		for(int i = 0; i < preds.size();i++){
			HSPLeafNode<T,K,R> temp = new HSPLeafNode<T,K,R>(globalRoot, preds.get(i));
			for(int j = 0; j < samples.size();j++){
				if(consistent(temp, samples.get(j), 2))
					temp.getKeyRecords().add(new Pair<K,R>(samples.get(j), null));
			}
			lowNodes.add(new Pair<HSPLeafNode<T,K,R>, Integer>(temp,2));
		}
		for(Pair<HSPLeafNode<T,K,R>, Integer> pair : lowNodes)
			globalRoot.getChildren().add(pair.getFirst());
		return lowNodes;
	}
	
	/**
	 * Takes the given leaf node and converts it to an index node with 
	 * leaf node children with the given predicates and the keys of the original
	 * leaf with be distributed to its index form's children by consistent
	 * @param toSplit The leaf node to split into an index node
	 * @param preds The predicates that the new index node's children will have
	 * @return An arraylist of pairs with the children of the new index node and their depths
	 */
	protected ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>> splitAndUpdate(Pair<HSPLeafNode<T,K,R>, Integer> toSplit, ArrayList<T> preds){
		ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>> lowNodes = new ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>>();
		HSPIndexNode<T,K,R> parent = new HSPIndexNode<T,K,R>(toSplit.getFirst().parent, toSplit.getFirst().predicate);
		if(parent.parent != null){
			((HSPIndexNode<T,K,R>)parent.parent).getChildren().remove(toSplit.getFirst());
			((HSPIndexNode<T,K,R>)parent.parent).getChildren().add(parent);
		}
		for(int i = 0; i < preds.size();i++){
			HSPLeafNode<T,K,R> temp = new HSPLeafNode<T,K,R>(parent, preds.get(i));
			for(int j = 0; j < toSplit.getFirst().getKeyRecords().size();j++){
				if(consistent(temp, toSplit.getFirst().getKeyRecords().get(j).getFirst(), 2))
					temp.getKeyRecords().add(toSplit.getFirst().getKeyRecords().get(j));
			}
			lowNodes.add(new Pair<HSPLeafNode<T,K,R>, Integer>(temp,toSplit.getSecond()+1));
		}
		for(Pair<HSPLeafNode<T,K,R>, Integer> pair : lowNodes)
			parent.getChildren().add(pair.getFirst());
		return lowNodes;
	}
	
	/**
	 * Converts all of the leaf nodes into reference nodes
	 * This conversion will take the nodes as in the order they are in and bind those
	 * to the corresponding output file for that partition
	 * @param toRefs A pair with the leaf nodes to be made into reference nodes and their depths
	 */
	protected void makeReferences(ArrayList<Pair<HSPLeafNode<T,K,R>, Integer>> toRefs){
		for(int i = 0; i < toRefs.size(); i++){
			HSPLeafNode<T,K,R> temp = toRefs.get(i).getFirst();
			partRoots.add(new Pair<T,Integer>(temp.getPredicate(), toRefs.get(i).getSecond()));
			HSPReferenceNode<T,K,R> ref = new HSPReferenceNode<T,K,R>(temp.parent, temp.predicate, new Path(String.format("part-r-%05d", i)));
			if(temp.parent != null){
				((HSPIndexNode<T,K,R>)temp.parent).getChildren().remove(toRefs.get(i).getFirst());
				((HSPIndexNode<T,K,R>)temp.parent).getChildren().add(ref);
				
			}
		}
	}
	
	/**
	 * This method has two possible uses:
	 * 1. Users with a fixed partition scheme for the root's children will give this method the default predicates
	 * and then call this method which will give globalRoot ReferenceNode children with those predicates
	 * <br>
	 * 2. Users with a flexible partition scheme will determine the root's children's predicates with the sampling
	 * and then add them to partitionPreds with depth 2 and then call this method 
	 * This method should only be called in the case of (numOfReducers - 1)/(numSpaceParts -1) == 1 
	 * and should be followed by return;
	 * @param preds 
	 */
	public void finalizeGlobalRoot(ArrayList<T> preds){
		ArrayList<HSPReferenceNode<T,K,R>> lowNodes = new ArrayList<HSPReferenceNode<T,K,R>>();
		for(int i = 0; i < preds.size();i++){
			lowNodes.add(new HSPReferenceNode<T,K,R>(globalRoot, preds.get(i), new Path(String.format("part-r-%05d", i))));
			partRoots.add(new Pair<T,Integer>(preds.get(i), 2));
		}
		globalRoot.getChildren().addAll(lowNodes);
	}
}
