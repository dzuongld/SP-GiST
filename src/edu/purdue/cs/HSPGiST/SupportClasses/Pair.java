package edu.purdue.cs.HSPGiST.SupportClasses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Java is silly and doesn't include a native pair class<br>
 * So we need to have one for our parser so it can emit values to the mapper<br>
 * The code was written by a forum user with no source information<br>
 * Irregular behavior is possible<br>
 * Additional code has been added as necessary; this includes implementations of
 * WritableComparable and Copyable all methods belonging to them are expected to
 * work correctly
 */
public class Pair<A, B> implements WritableComparable<Pair<A, B>>,
		Copyable<Pair<A, B>>, Sized {
	private A first;
	private B second;

	/**
	 * Default Constructor
	 */
	public Pair() {
		super();
	}

	/**
	 * Construct a new pair
	 * 
	 * @param first
	 *            The first part of the pair
	 * @param second
	 *            The second part of the pair
	 */
	public Pair(A first, B second) {
		super();
		this.first = first;
		this.second = second;
	}

	/*
	 * Setter and getters for pair components
	 */

	public A getFirst() {
		return first;
	}

	public void setFirst(A first) {
		this.first = first;
	}

	public B getSecond() {
		return second;
	}

	public void setSecond(B second) {
		this.second = second;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Pair) {
			@SuppressWarnings("rawtypes")
			Pair otherPair = (Pair) other;
			return ((this.first == otherPair.first || (this.first != null
					&& otherPair.first != null && this.first
						.equals(otherPair.first))) && (this.second == otherPair.second || (this.second != null
					&& otherPair.second != null && this.second
						.equals(otherPair.second))));
		}

		return false;
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		if (this.equals(o))
			return 0;
		int comp = first.toString().compareTo(o.first.toString());
		if (comp == 0)
			return second.toString().compareTo(o.second.toString());
		return comp;
	}

	/**
	 * Return a hash of this object (This is the only dubious method)
	 */
	@Override
	public int hashCode() {
		int hashFirst = first != null ? first.hashCode() : 0;
		int hashSecond = second != null ? second.hashCode() : 0;
		return (hashFirst + hashSecond) * hashSecond + hashFirst;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("(");
		return sb.append(first.toString()).append(", ")
				.append(second.toString()).append(")").toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// Read class names to initialize pair
		String temp = arg0.readUTF();
		try {
			Class<A> clazz = (Class<A>) Class.forName(temp);
			A obj = clazz.newInstance();
			((WritableComparable<A>) obj).readFields(arg0);
			first = obj;
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			first = null;
		}
		temp = arg0.readUTF();
		try {
			Class<B> clazz = (Class<B>) Class.forName(temp);
			B obj = clazz.newInstance();
			((WritableComparable<B>) obj).readFields(arg0);
			second = obj;
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			second = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput arg0) throws IOException {
		// Write class names to write pair
		arg0.writeUTF(first.getClass().getName());
		((WritableComparable<A>) first).write(arg0);
		arg0.writeUTF(second.getClass().getName());
		((WritableComparable<B>) second).write(arg0);
	}

	@Override
	public Pair<A, B> copy() {
		return new Pair<A, B>(first, second);
	}

	@Override
	public long getSize() {
		//2*2 bytes for UTF length, lengths of class names, and the sizes of the predicates themselves
		return 4 + first.getClass().getName().length()
				+ second.getClass().getName().length()
				+ ((Sized) first).getSize() + ((Sized) second).getSize();
	}
}
