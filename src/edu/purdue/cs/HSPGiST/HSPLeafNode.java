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
 */
public class HSPLeafNode<T,K> extends HSPNode<T,K> implements WritableComparable<HSPLeafNode<T,K>>{
	public ArrayList<K> keys;
	
	public HSPLeafNode(HSPIndexNode<T,K> parent){
		setParent(parent);
		keys = new ArrayList<K>();
	}
	public HSPLeafNode(HSPIndexNode<T,K> parent,T predicate){
		setParent(parent);
		keys = new ArrayList<K>();
		setPredicate(predicate);
	}
	public HSPLeafNode(HSPIndexNode<T,K> parent,ArrayList<K> keys, T predicate){
		setParent(parent);
		this.keys = keys;
		setPredicate(predicate);
	}
	public String toString() {
		if(getParent() == null)
			return "";
		StringBuilder stren = new StringBuilder("Keys:");
		for(K key : keys)
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
	public int compareTo(HSPLeafNode<T, K> o) {
		return this.keys.toString().compareTo(o.keys.toString());
	}
}
