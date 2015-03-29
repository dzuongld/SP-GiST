package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;

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
	public HSPLeafNode(){
		setParent(null);
		keys = new ArrayList<Pair<K,R>>();
	}
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
		if(keys.size() == 0)
			return "";
		StringBuilder stren = new StringBuilder("Keys:");
		for(Pair<K,R> key : keys)
			stren.append(key.toString());
		return stren.toString();
	}
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		keys = new ArrayList<Pair<K,R>>();
		int count = arg0.readInt();
		String temp = arg0.readUTF();
		try {
			Class<T> clazz = (Class<T>) Class.forName(temp);
			T obj = clazz.newInstance();
			((WritableComparable<T>)obj).readFields(arg0);
			setPredicate(obj);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			setPredicate(null);
		}
		for(int i = 0; i < count; i++){
			Pair<K,R> adder = new Pair<K,R>();
			adder.readFields(arg0);
			keys.add(adder);
		}
	}
	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(keys.size());
		if(getPredicate() == null)
			arg0.writeUTF("empty");
		else{
			arg0.writeUTF(getPredicate().getClass().getName());
			((WritableComparable<T>)getPredicate()).write(arg0);
		}
		for(Pair<K,R> datum : keys){
			datum.write(arg0);
		}
	}
	@Override
	public int compareTo(HSPLeafNode<T, K,R> o) {
		//TODO: Implement more robust compareTo method
		return this.keys.toString().compareTo(o.keys.toString());
	}
	@Override
	public HSPNode<T, K, R> copy() {
		return new HSPLeafNode<T,K,R>((HSPIndexNode<T, K, R>) getParent(), keys, getPredicate());
	}
}
