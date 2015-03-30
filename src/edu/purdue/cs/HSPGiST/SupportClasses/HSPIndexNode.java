package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;

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
 * @param <R> Record type
 */
public class HSPIndexNode<T,K,R>  extends HSPNode<T,K,R> implements WritableComparable<HSPIndexNode<T,K,R>>{
	public ArrayList<HSPNode<T,K,R>> children;
	//Hadoop needs this default constructor to run correctly
	public HSPIndexNode(){
		children = new ArrayList<HSPNode<T,K,R>>();
	}
	public HSPIndexNode(T predicate,HSPNode<T,K,R> parent){
		children = new ArrayList<HSPNode<T,K,R>>();
		setPredicate(predicate);
		setParent(parent);
	}
	
	public HSPIndexNode(ArrayList<HSPNode<T,K,R>> children,HSPNode<T,K,R> parent){
		this.children = children;
		setParent(parent);
	}
	
	public HSPIndexNode(T predicate,HSPNode<T,K,R> parent, ArrayList<HSPNode<T,K,R>> children){
		this.children = new ArrayList<HSPNode<T,K,R>>();
		for(HSPNode<T,K,R> child : children){
			this.children.add(((Copyable<HSPNode<T,K,R>>)child).copy());
		}
		setPredicate(predicate);
		setParent(parent);
	}
	
	public HSPIndexNode(HSPNode<T,K,R> parent, HSPIndex<T,K,R> index, int level){
		children = new ArrayList<HSPNode<T,K,R>>();
		setParent(parent);
		if(!index.nodeShrink && index.path == HSPIndex.PathShrink.NEVER){
			//Picksplit isn't aware that this is an empty leafnode and will try to split it
			//This lets us piggyback on it to determine the full list of predicates for a node's children
			//This type of index node creation only happens with PathShrink == NEVER 
			//PathShrink == NEVER disallows data-driven space partitioning which makes this okay
			//Pretend we are splitting this node being made to construct its children
			ArrayList<ArrayList<Pair<K, R>>> junk = new ArrayList<ArrayList<Pair<K, R>>>();
			for(int i = 0; i < index.numSpaceParts; i++){
				junk.add(new ArrayList<Pair<K, R>>());
			}
			ArrayList<T> preds = new ArrayList<T>();
			index.picksplit(new HSPLeafNode<T,K,R>((HSPIndexNode<T, K,R>) parent), level, junk, preds);
			for(int i = 0; i < index.numSpaceParts; i++){
				children.add(new HSPLeafNode<T,K,R>(this, preds.get(i)));
			}
		}
	}
	/**
	 * Used only in GlobalReducer, don't use it anywhere else
	 */
	@SuppressWarnings("unchecked")
	public HSPIndexNode(HSPIndex index, int level, T predicate, HSPNode<T,K,R> parent, int depth){
		children = new ArrayList<HSPNode<T,K,R>>();
		setParent(parent);
		setPredicate(predicate);
		ArrayList<HSPNode<T,K,R>> stack = new ArrayList<HSPNode<T,K,R>>();
		ArrayList<ArrayList<Pair<K, R>>> junk = new ArrayList<ArrayList<Pair<K, R>>>();
		for(int i = 0; i < index.numSpaceParts; i++){
			junk.add(new ArrayList<Pair<K, R>>());
		}
		ArrayList<T> preds = new ArrayList<T>();
		index.picksplit(new HSPLeafNode<T,K,R>(this, getPredicate()), depth, junk, preds);
		if(depth == level){
			for(int i = 0; i < index.numSpaceParts; i++){
				children.add(new HSPLeafNode<T,K,R>(this, preds.get(i)));
			}
		}
		else{
			for(int i = 0; i < index.numSpaceParts; i++){
				children.add(new HSPIndexNode<T,K,R>(index, level, preds.get(i), this, depth+1));
			}
		}
		
	}
	public HSPIndexNode(T predicate, HSPNode<T,K,R> parent, HSPIndex<T,K,R> index, int level){
		children = new ArrayList<HSPNode<T,K,R>>();
		setParent(parent);
		setPredicate(predicate);
		if(!index.nodeShrink && index.path == HSPIndex.PathShrink.NEVER){
			//Picksplit isn't aware that this is an empty leafnode and will try to split it
			//This lets us piggyback on it to determine the full list of predicates for a node's children
			//This type of index node creation only happens with PathShrink == NEVER 
			//PathShrink == NEVER disallows data-driven space partitioning which makes this okay
			//Pretend we are splitting this node being made to construct its children
			ArrayList<ArrayList<Pair<K, R>>> junk = new ArrayList<ArrayList<Pair<K, R>>>();
			for(int i = 0; i < index.numSpaceParts; i++){
				junk.add(new ArrayList<Pair<K, R>>());
			}
			ArrayList<T> preds = new ArrayList<T>();
			index.picksplit(new HSPLeafNode<T,K,R>((HSPIndexNode<T, K,R>) parent, predicate), level, junk, preds);
			for(int i = 0; i < index.numSpaceParts; i++){
				children.add(new HSPLeafNode<T,K,R>(this, preds.get(i)));
			}
		}
	}
	
	public int compareTo(HSPIndexNode<T,K,R> o){
		//TODO: implement more robust comparator
		return this.children.toString().compareTo(o.children.toString());
	}
	public String toString() {
		if(children == null || children.size()==0)
			return "";
		if(getParent() == null && getPredicate() == null){
			return "Root Node";
		}
		else if(getParent() == null){
			return "Root Node Predicate: " + getPredicate().toString();
		}
		return "Predicate: " + getPredicate().toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		int size = arg0.readInt();
		for(int i = 0; i < size; i++){
			//populate node with dummy children to get right size
			children.add(new HSPIndexNode<T, K, R>( (T) null, this));
		}
		String temp = arg0.readUTF();
		try {
			Class<T> clazz = (Class<T>) Class.forName(temp);
			T obj = clazz.newInstance();
			((WritableComparable<T>)obj).readFields(arg0);
			setPredicate(obj);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			setPredicate(null);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		if(children != null)
			arg0.writeInt(children.size());
		else
			arg0.writeInt(0);
		if(getPredicate() == null)
			arg0.writeUTF("empty");
		else{
			arg0.writeUTF(getPredicate().getClass().getName());
			((WritableComparable<T>)getPredicate()).write(arg0);
		}
	}
	@Override
	public HSPNode<T, K, R> copy() {
		return new HSPIndexNode<T,K,R>(getPredicate(), getParent(), children);
	}
}
