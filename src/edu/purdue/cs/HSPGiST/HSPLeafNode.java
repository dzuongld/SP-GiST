package edu.purdue.cs.HSPGiST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

/**
 * Representation of leaf or data nodes in an index
 * 
 * @author Stefan Brinton & Daniel Fortney
 *
 * @param <T> Predicate Type
 * @param <K> Key Type
 * @param <R> Record Type
 */
public class HSPLeafNode<T,K,R> extends HSPNode<T,K,R> implements WritableComparable<HSPLeafNode<T,K,R>>{
	public ArrayList<Pair<K,R>> keys;
	public ArrayList<R> records;
	public HSPLeafNode(HSPIndexNode<T,K,R> parent){
		setParent(parent);
		keys = new ArrayList<Pair<K,R>>();
	}
	public HSPLeafNode(HSPIndexNode<T,K,R> parent,T predicate){
		setParent(parent);
		keys = new ArrayList<Pair<K,R>>();
		setPredicate(predicate);
	}
	public HSPLeafNode(HSPIndexNode<T,K,R> parent,ArrayList<Pair<K,R>> keys, T predicate){
		setParent(parent);
		this.keys = keys;
		setPredicate(predicate);
	}
	public String toString() {
		if(getParent() == null)
			return "";
		StringBuilder stren = new StringBuilder("Keys:");
		for(Pair<K,R> key : keys)
			stren.append(key.toString() + "\n");
		return stren.toString();
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int compareTo(HSPLeafNode<T, K,R> o) {
		//TODO: Implement more robust compareTo method
		return this.keys.toString().compareTo(o.keys.toString());
	}
}
