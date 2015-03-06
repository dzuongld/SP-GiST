package edu.purdue.cs.HSPGiST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

/**
 * Representation of index nodes for HSP-GiST indices
 * These only store pointers to children node and a predicate
 * User Defined methods picksplit and consistent shouldn't
 * access need to access the children of these nodes
 * 
 * @author Stefan Brinton & Daniel Fortney
 *
 * @param <T> Predicate type
 * @param <K> Key type
 */
public class HSPIndexNode<T,K>  extends HSPNode<T,K> implements WritableComparable<HSPIndexNode<T,K>>{
	public ArrayList<HSPNode<T,K>> children;
	//Hadoop needs this default constructor to run correctly
	public HSPIndexNode(){
		children = new ArrayList<HSPNode<T,K>>();
	}
	public HSPIndexNode(T predicate,HSPNode<T,K> parent){
		children = new ArrayList<HSPNode<T,K>>();
		setPredicate(predicate);
		setParent(parent);
	}
	
	public HSPIndexNode(ArrayList<HSPNode<T,K>> children,HSPNode<T,K> parent){
		this.children = children;
		setParent(parent);
	}
	
	
	
	public HSPIndexNode(HSPNode<T,K> parent, HSPIndex<T,K> index, int level){
		children = new ArrayList<HSPNode<T,K>>();
		setParent(parent);
		if(!index.nodeShrink && index.path == HSPIndex.PathShrink.NEVER){
			//Picksplit isn't aware that this is an empty leafnode and will try to split it
			//This lets us piggyback on it to determine the full list of predicates for a node's children
			//This type of index node creation only happens with PathShrink == NEVER 
			//PathShrink == NEVER disallows data-driven space partitioning which makes this okay
			//Pretend we are splitting this node being made to construct its children
			ArrayList<ArrayList<K>> junk = new ArrayList<ArrayList<K>>();
			for(int i = 0; i < index.numSpaceParts; i++){
				junk.add(new ArrayList<K>());
			}
			ArrayList<T> preds = new ArrayList<T>();
			index.picksplit(new HSPLeafNode<T,K>((HSPIndexNode<T, K>) parent), level, junk, preds);
			for(int i = 0; i < index.numSpaceParts; i++){
				children.add(new HSPLeafNode<T,K>(this, preds.get(i)));
			}
		}
	}
	public HSPIndexNode(T predicate, HSPNode<T,K> parent, HSPIndex<T,K> index, int level){
		children = new ArrayList<HSPNode<T,K>>();
		setParent(parent);
		setPredicate(predicate);
		if(!index.nodeShrink && index.path == HSPIndex.PathShrink.NEVER){
			//Picksplit isn't aware that this is an empty leafnode and will try to split it
			//This lets us piggyback on it to determine the full list of predicates for a node's children
			//This type of index node creation only happens with PathShrink == NEVER 
			//PathShrink == NEVER disallows data-driven space partitioning which makes this okay
			//Pretend we are splitting this node being made to construct its children
			ArrayList<ArrayList<K>> junk = new ArrayList<ArrayList<K>>();
			for(int i = 0; i < index.numSpaceParts; i++){
				junk.add(new ArrayList<K>());
			}
			ArrayList<T> preds = new ArrayList<T>();
			index.picksplit(new HSPLeafNode<T,K>((HSPIndexNode<T, K>) parent, predicate), level, junk, preds);
			for(int i = 0; i < index.numSpaceParts; i++){
				children.add(new HSPLeafNode<T,K>(this, preds.get(i)));
			}
		}
	}
	
	public int compareTo(HSPIndexNode<T,K> o){
		return this.children.toString().compareTo(o.children.toString());
	}
	public String toString() {
		if(children == null)
			return "";
		if(getParent() == null){
			return "Root Node";
		}
		
		return "Predicate: " + getPredicate().toString() +"\n";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
