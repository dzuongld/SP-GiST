package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;

/**
 * These are the leaf nodes of the global index, providing paths to "local" index files
 * @author Stefan Brinton
 *
 * @param <T> Predicate type
 * @param <K> Key type
 * @param <R> Record type
 */
public class HSPReferenceNode<T,K,R> extends HSPNode<T,K,R> implements WritableComparable<HSPReferenceNode<T,K,R>>{
	public Path reference;
	
	public HSPReferenceNode(){
		reference = null;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public HSPReferenceNode(HSPNode parent, T predicate, Path path){
		reference = path;
		setPredicate(predicate);
		setParent(parent);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public HSPReferenceNode(HSPNode parent, T predicate, String path){
		reference = new Path(path);
		setPredicate(predicate);
		setParent(parent);
	}
	
	@Override
	public HSPNode<T, K, R> copy() {
		return new HSPReferenceNode<T,K,R>(getParent(),getPredicate(),reference);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		String temp = arg0.readUTF();
		try {
			Class<T> clazz = (Class<T>) Class.forName(temp);
			T obj = clazz.newInstance();
			((WritableComparable<T>)obj).readFields(arg0);
			setPredicate(obj);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			setPredicate(null);
		}
		reference = new Path(arg0.readUTF());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		if(getPredicate() == null)
			arg0.writeUTF("empty");
		else{
			arg0.writeUTF(getPredicate().getClass().getName());
			((WritableComparable<T>)getPredicate()).write(arg0);
		}
		arg0.writeUTF(reference.toString());
	}

	@Override
	public int compareTo(HSPReferenceNode<T, K,R> arg0) {
		return reference.toString().compareTo(arg0.toString());
	}
	
	public String toString(){
		if(reference == null){
			return "";
		}
		return getPredicate().toString() +" " + reference.toString();
	}
}
