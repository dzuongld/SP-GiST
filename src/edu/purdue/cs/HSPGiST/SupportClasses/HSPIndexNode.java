package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;

/**
 * Representation of index nodes for HSP-GiST indices They store a reference to
 * their parent, children, and predicate
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
public class HSPIndexNode<T, K, R> extends HSPNode<T, K, R> implements
		WritableComparable<HSPIndexNode<T, K, R>> {

	/**
	 * This node's children
	 */
	private ArrayList<HSPNode<T, K, R>> children;

	/**
	 * Empty constructor for initializing this object from its class object or
	 * for creating an empty Index Node (root)
	 */
	public HSPIndexNode() {
		this.children = new ArrayList<HSPNode<T, K, R>>();
		parent = null;
		predicate = null;
	}

	/**
	 * Construct a childless index node
	 * 
	 * @param parent
	 *            The parent of this node
	 * @param predicate
	 *            The predicate of this node
	 */
	public HSPIndexNode(HSPNode<T, K, R> parent, T predicate) {
		this.children = new ArrayList<HSPNode<T, K, R>>();
		this.predicate = predicate;
		this.parent = parent;
	}

	/**
	 * This is a null constructor for creating a totally null HSPIndexNode<br>
	 * This is used to print "nothing" to output when writing a LocalIndex
	 * (context.write() does not allow null args it will NPE)
	 * 
	 * @param parent
	 *            should be the value null
	 */
	public HSPIndexNode(HSPNode<T, K, R> parent) {
		this.children = null;
		this.parent = parent;
		this.predicate = null;
	}

	/**
	 * Used to create a copy of a node
	 * 
	 * @param parent
	 *            The parent of this node
	 * @param predicate
	 *            The predicate of this node
	 * @param children
	 *            The children of this node
	 */
	public HSPIndexNode(HSPNode<T, K, R> parent, T predicate,
			ArrayList<HSPNode<T, K, R>> children) {
		this.children = new ArrayList<HSPNode<T, K, R>>();
		for (HSPNode<T, K, R> child : children) {
			this.children.add(((Copyable<HSPNode<T, K, R>>) child).copy());
		}
		this.predicate = predicate;
		this.parent = parent;
	}

	/**
	 * Used to construct indexNodes while performing insertion<br>
	 * Reconsider what you're doing if you try to call this elsewhere
	 * 
	 * @param parent
	 *            The parent of the node
	 * @param predicate
	 *            The predicate of the node
	 * @param index
	 *            The type of index this node is a part of
	 * @param level
	 *            The level within the tree the node will be created at
	 */
	public HSPIndexNode(HSPNode<T, K, R> parent, T predicate,
			HSPIndex<T, K, R> index, int level) {
		this.children = new ArrayList<HSPNode<T, K, R>>();
		this.parent = parent;
		this.predicate = predicate;
		if (!index.nodeShrink && index.path == HSPIndex.PathShrink.NEVER) {
			/*
			 * Setup the arrayLists for picksplit picksplit is necessary to
			 * determine the predicates for this node's children when we are
			 * doing a nodeShrink == false && pathShrink == NEVER as any index
			 * node that exists is assumed to have a full complement of children
			 * (This isn't the case for nodeShrink == true as missing children
			 * will be added as necessary) This is the same as splitting an
			 * overfull leaf but with an index node
			 */
			ArrayList<ArrayList<Pair<K, R>>> junk = new ArrayList<ArrayList<Pair<K, R>>>();
			for (int i = 0; i < index.numSpaceParts; i++) {
				junk.add(new ArrayList<Pair<K, R>>());
			}
			ArrayList<T> preds = new ArrayList<T>();
			index.picksplit(new HSPLeafNode<T, K, R>(
					(HSPIndexNode<T, K, R>) parent, predicate), level, junk,
					preds);
			for (int i = 0; i < index.numSpaceParts; i++) {
				this.children.add(new HSPLeafNode<T, K, R>(this, preds.get(i)));
			}
		}
	}

	/**
	 * @return the children of this node
	 */
	public ArrayList<HSPNode<T, K, R>> getChildren() {
		return children;
	}

	/**
	 * @param children
	 *            the children to set
	 */
	public void setChildren(ArrayList<HSPNode<T, K, R>> children) {
		this.children = children;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (!(o instanceof HSPIndexNode<?, ?, ?>))
			return false;
		HSPIndexNode<T, K, R> other;
		try {
			other = (HSPIndexNode<T, K, R>) o;
		} catch (ClassCastException e) {
			return false;
		}
		if ((children == other.children || this.children != null
				&& other.children != null
				&& this.children.equals(other.children))
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
	public int compareTo(HSPIndexNode<T, K, R> o) {
		if (o == null)
			return 1;
		if (this.equals(o))
			return 0;
		if (o.children == null)
			return 1;
		return children.size() < o.children.size() ? -1 : 1;
	}

	@Override
	public String toString() {
		if (this.children == null || this.children.size() == 0)
			return "";
		if (this.parent == null && this.predicate == null) {
			return "Root Node";
		} else if (parent == null) {
			StringBuilder sb = new StringBuilder("Local Root Node Predicate: ");
			sb.append(predicate.toString());
			return sb.toString();
		}
		StringBuilder sb = new StringBuilder("Predicate: ");
		sb.append(predicate.toString());
		return sb.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		int size = arg0.readInt();
		for (int i = 0; i < size; i++) {
			// populate node with dummy children to get right size
			this.children.add(new HSPIndexNode<T, K, R>(this, (T) null));
		}
		// Read name of predicate class to create this Index Node's predicate
		String temp = arg0.readUTF();
		try {
			Class<T> clazz = (Class<T>) Class.forName(temp);
			T obj = clazz.newInstance();
			((WritableComparable<T>) obj).readFields(arg0);
			this.predicate = obj;
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			// Additionally catches "null" class
			this.predicate = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		if (this.children != null)
			arg0.writeInt(this.children.size());
		else
			arg0.writeInt(0);
		if (this.predicate == null)
			arg0.writeUTF("null");
		else {
			// Write predicate class name so we can invoke one when reading this
			// back
			arg0.writeUTF(this.predicate.getClass().getName());
			((WritableComparable<T>) this.predicate).write(arg0);
		}
	}

	@Override
	public HSPNode<T, K, R> copy() {
		return new HSPIndexNode<T, K, R>(parent, this.predicate, this.children);
	}
}
