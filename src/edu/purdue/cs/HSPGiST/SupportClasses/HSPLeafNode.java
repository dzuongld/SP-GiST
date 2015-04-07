package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;

/**
 * Representation of leaf or data nodes in an index Contains a reference to
 * parent, predicate, and key-record pairs
 * 
 * @author Stefan Brinton
 *
 * @param <T>
 *            Predicate Type
 * @param <K>
 *            Key Type
 * @param <R>
 *            Record Type
 */
public class HSPLeafNode<T, K, R> extends HSPNode<T, K, R> implements
		WritableComparable<HSPLeafNode<T, K, R>> {

	/**
	 * This nodes key-record pairs
	 */
	private ArrayList<Pair<K, R>> keyRecords;

	/**
	 * Empty constructor for initialization from class name
	 */
	public HSPLeafNode() {
		parent = null;
		keyRecords = new ArrayList<Pair<K, R>>();
	}

	/**
	 * Null leaf constructor for outputing an empty leaf
	 * 
	 * @param parent
	 *            Should be null
	 */
	public HSPLeafNode(HSPIndexNode<T, K, R> parent) {
		this.parent = parent;
		keyRecords = new ArrayList<Pair<K, R>>();
	}

	/**
	 * Constructor for a recordless leaf
	 * 
	 * @param parent
	 *            The parent of this leaf
	 * @param predicate
	 *            The predicate of this leaf
	 */
	public HSPLeafNode(HSPIndexNode<T, K, R> parent, T predicate) {
		this.parent = parent;
		keyRecords = new ArrayList<Pair<K, R>>();
		this.predicate = predicate;
	}

	/**
	 * Constructor for a copied leaf, or a leaf resulting from a split
	 * 
	 * @param parent
	 *            The parent of this node
	 * @param predicate
	 *            The predicate of this node
	 * @param records
	 *            The records belonging to this leaf
	 */
	public HSPLeafNode(HSPIndexNode<T, K, R> parent, T predicate,
			ArrayList<Pair<K, R>> records) {
		this.parent = parent;
		keyRecords = records;
		this.predicate = predicate;
	}

	/**
	 * @return the keyRecords of this node
	 */
	public ArrayList<Pair<K, R>> getKeyRecords() {
		return keyRecords;
	}

	/**
	 * @param keyRecords
	 *            the keyRecords to give this leaf
	 */
	public void setKeyRecords(ArrayList<Pair<K, R>> keyRecords) {
		this.keyRecords = keyRecords;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (!(o instanceof HSPLeafNode<?, ?, ?>))
			return false;
		HSPLeafNode<T, K, R> other;
		try {
			other = (HSPLeafNode<T, K, R>) o;
		} catch (ClassCastException e) {
			return false;
		}
		if ((keyRecords == other.keyRecords || keyRecords != null
				&& other.keyRecords != null
				&& this.keyRecords.equals(other.keyRecords))
				&& (predicate == other.predicate || this.predicate != null
						&& other.predicate != null
						&& this.predicate.equals(other.predicate))
				&& (parent == other.parent || this.parent != null
						&& other.parent != null
						&& this.parent.equals(other.parent)))
			return true;
		return false;
	}

	@Override
	public int compareTo(HSPLeafNode<T, K, R> o) {
		if (o == null)
			return 1;
		if (this.equals(o))
			return 0;
		if (o.keyRecords == null)
			return 1;
		return keyRecords.size() < o.keyRecords.size() ? -1 : 1;
	}

	@Override
	public String toString() {
		if (keyRecords.size() == 0)
			return "";
		StringBuilder stren = new StringBuilder("Keys:");
		for (Pair<K, R> key : keyRecords)
			stren.append(key.toString());
		return stren.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		size = arg0.readLong();
		
		// Get class of predicate to read its fields and set this predicate
		String temp = arg0.readUTF();
		try {
			Class<T> clazz = (Class<T>) Class.forName(temp);
			T obj = clazz.newInstance();
			((WritableComparable<T>) obj).readFields(arg0);
			setPredicate(obj);
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			// Additionally catches "null" class
			setPredicate(null);
		}
		setOffset(arg0.readInt());
		// Read data for this node
		keyRecords = new ArrayList<Pair<K, R>>();
		int count = arg0.readInt();
		for (int i = 0; i < count; i++) {
			Pair<K, R> adder = new Pair<K, R>();
			adder.readFields(arg0);
			keyRecords.add(adder);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeBoolean(false);
		arg0.writeLong(size);
		
		// Write predicate class name so we can invoke one when reading this
		// back
		if (getPredicate() == null)
			arg0.writeUTF("null");
		else {
			arg0.writeUTF(getPredicate().getClass().getName());
			((WritableComparable<T>) getPredicate()).write(arg0);
		}
		arg0.writeInt(getOffset());
		arg0.writeInt(keyRecords.size());
		for (Pair<K, R> datum : keyRecords) {
			datum.write(arg0);
		}
	}

	@Override
	public HSPNode<T, K, R> copy() {
		return new HSPLeafNode<T, K, R>((HSPIndexNode<T, K, R>) getParent(),
				getPredicate(), keyRecords);
	}

	@Override
	public long getSize() {
		for(Pair<K,R> datum : keyRecords)
			size += ((Sized)datum).getSize();
		size += Integer.SIZE>>3;
		long selfSize = (Integer.SIZE>>3) + (Long.SIZE>>3) + 1;
		if(predicate == null)
			selfSize += 6; //UTF outputs 2 bytes for length of string then the string is 4 bytes
		else{
			//2 for UTF output length, length of UTF string, and the predicate's size
			selfSize += 2 + predicate.getClass().getName().length() + ((Sized)predicate).getSize();
		}
		return size + selfSize;
	}
}
