package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;

/**
 * These are the leaf nodes of the global index, providing paths to "local"
 * index files
 * 
 * @author Stefan Brinton
 *
 * @param <T>
 *            Predicate type
 * @param <K>
 *            Key type
 * @param <R>
 *            Record type
 */
public class HSPReferenceNode<T, K, R> extends HSPNode<T, K, R> implements
		WritableComparable<HSPReferenceNode<T, K, R>> {
	private Path reference;

	/**
	 * Empty Constructor for initializing from class name
	 */
	public HSPReferenceNode() {
		reference = null;
	}

	/**
	 * Constructor for a reference node from a path
	 * 
	 * @param parent
	 *            The parent of the node
	 * @param predicate
	 *            The predicate of the node
	 * @param path
	 *            The path of the local root the node represents
	 */
	public HSPReferenceNode(HSPNode<T, K, R> parent, T predicate, Path path) {
		reference = path;
		this.predicate = predicate;
		this.parent = parent;
	}

	/**
	 * Constructor for a reference node from a string
	 * 
	 * @param parent
	 *            The parent of the node
	 * @param predicate
	 *            The predicate of the node
	 * @param path
	 *            The string for the path of the local root the node represents
	 */
	public HSPReferenceNode(HSPNode<T, K, R> parent, T predicate, String path) {
		this(parent, predicate, new Path(path));
	}

	/**
	 * @return the reference this node has
	 */
	public Path getReference() {
		return reference;
	}

	/**
	 * @param reference
	 *            the reference to give this node
	 */
	public void setReference(Path reference) {
		this.reference = reference;
	}

	/**
	 * @return the number associated with this nodes reference file
	 */
	public int getFileNumber() {
		String[] split = reference.toString().split("-");
		return Integer.parseInt(split[split.length - 1]);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (!(o instanceof HSPReferenceNode<?, ?, ?>))
			return false;
		HSPReferenceNode<T, K, R> other;
		try {
			other = (HSPReferenceNode<T, K, R>) o;
		} catch (ClassCastException e) {
			return false;
		}
		if ((reference == other.reference || this.reference != null
				&& other.reference != null
				&& this.reference.equals(other.reference))
				&& (predicate == other.predicate || this.predicate != null
						&& other.predicate != null
						&& this.predicate.equals(other.predicate))
				&& (parent == other.parent || this.parent != null
						&& other.parent != null
						&& this.parent.equals(other.parent)))
			return true;
		return false;
	}

	/*
	 * Trivial Comparator for IndexNodes for WritableComparable
	 */
	@Override
	public int compareTo(HSPReferenceNode<T, K, R> o) {
		if (o == null)
			return 1;
		if (this.equals(o))
			return 0;
		if (o.reference == null)
			return 1;
		return getFileNumber() < o.getFileNumber() ? -1 : 1;
	}

	@Override
	public String toString() {
		if (reference == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder(predicate.toString());
		return sb.append(" ").append(reference.toString()).toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		String temp = arg0.readUTF();
		// Read the class name to initialize this node's predicate
		try {
			Class<T> clazz = (Class<T>) Class.forName(temp);
			T obj = clazz.newInstance();
			((WritableComparable<T>) obj).readFields(arg0);
			predicate = obj;
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			// Catches "null" class initialization
			predicate = null;
		}
		reference = new Path(arg0.readUTF());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		// Write class name or null for read back
		if (predicate == null)
			arg0.writeUTF("null");
		else {
			arg0.writeUTF(predicate.getClass().getName());
			((WritableComparable<T>) predicate).write(arg0);
		}
		arg0.writeUTF(reference.toString());
	}

	@Override
	public HSPNode<T, K, R> copy() {
		return new HSPReferenceNode<T, K, R>(parent, predicate, reference);
	}
}
