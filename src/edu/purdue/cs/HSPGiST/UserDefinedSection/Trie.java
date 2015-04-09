package edu.purdue.cs.HSPGiST.UserDefinedSection;

import java.util.ArrayList;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.SupportClasses.*;

/**
 * Basic implementation of a trie
 * @author Dan Fortney
 *
 */

//The node predicate is a letter. The data nodes are strings.
public class Trie<R> extends HSPIndex<WritableChar,WritableString,R>{
	Trie(){
		numSpaceParts = 26;
		resolution = 50;
		path = PathShrink.LEAF;
		nodeShrink = false;
	}
	
	@Override
	public boolean consistent(HSPNode<WritableChar, WritableString,R> e,
			WritableString q, int level) {
		if (((WritableChar)(e.getPredicate())).getChar() == q.getString().charAt(level-1)) return true;
		if (((WritableChar)(e.getPredicate())).getChar() == '-' && level >= q.getString().length()) return true;
		return false;
	}
	
	private void initializePredicates(ArrayList<WritableChar> predicates){
		for (int i=0; i<27; i++){
			predicates.add(new WritableChar(intToChar(i)));
		}	
	}
	
	private char intToChar(int i){
		switch (i) {
			case 0: return '-';
			case 1: return 'a';
			case 2: return 'b';
			case 3: return 'c';
			case 4: return 'd';
			case 5: return 'e';
			case 6: return 'f';
			case 7: return 'g';
			case 8: return 'h';
			case 9: return 'i';
			case 10: return 'j';
			case 11: return 'k';
			case 12: return 'l';
			case 13: return 'm';
			case 14: return 'n';
			case 15: return 'o';
			case 16: return 'p';
			case 17: return 'q';
			case 18: return 'r';
			case 19: return 's';
			case 20: return 't';
			case 21: return 'u';
			case 22: return 'v';
			case 23: return 'w';
			case 24: return 'x';
			case 25: return 'y';
			case 26: return 'z';
		}
		return '-';
	}
	
	private int charToInt(char c){
		switch (c) {
			case '-': return 0;
			case 'a': return 1;
			case 'b': return 2;
			case 'c': return 3;
			case 'd': return 4;
			case 'e': return 5;
			case 'f': return 6;
			case 'g': return 7;
			case 'h': return 8;
			case 'i': return 9;
			case 'j': return 10;
			case 'k': return 11;
			case 'l': return 12;
			case 'm': return 13;
			case 'n': return 14;
			case 'o': return 15;
			case 'p': return 16;
			case 'q': return 17;
			case 'r': return 18;
			case 's': return 19;
			case 't': return 20;
			case 'u': return 21;
			case 'v': return 22;
			case 'w': return 23;
			case 'x': return 24;
			case 'y': return 25;
			case 'z': return 26;
		}
		return -1;
	}
		
	@Override
	public boolean picksplit(
			HSPLeafNode<WritableChar, WritableString, R> leaf, int level,
			ArrayList<ArrayList<Pair<WritableString, R>>> childrenData,
			ArrayList<WritableChar> childrenPredicates) {
		if (level == 1){
			initializePredicates(childrenPredicates);
			for (Pair<WritableString, R> p : leaf.getKeyRecords()){
				WritableString str = p.getFirst();
				int idx = charToInt(str.getString().charAt(0));
				childrenData.get(idx).add(p);
			}
			for (int i=0; i<childrenData.size(); i++){
				if (childrenData.get(i).size() > 1){
					return true;
				}
			}
			return false;
		}
		WritableChar predic = leaf.getPredicate();
		if (predic == null) return false;
		initializePredicates(childrenPredicates);
		for (Pair<WritableString, R> p : leaf.getKeyRecords()){
			WritableString str = p.getFirst();
			int idx = charToInt(str.getString().charAt(level-1));
			childrenData.get(idx).add(p);
		}
		for (int i=0; i<childrenData.size(); i++){
			if (childrenData.get(i).size() > 1){
				return true;
			}
		}
		return false;
	}

	@Override
	public WritableChar determinePredicate(WritableString key,
			WritableChar parentPred, int level) {
		char c = key.getString().charAt(level-1);
		return new WritableChar(c);
	}
	
	@Override
	public boolean consistent(WritableChar e, WritableString q, int level) {
		if (e.getChar() == q.getString().charAt(level - 1)) return true;
		if (e.getChar() == '-' && level >= q.getString().length()) return true;
		return false;
	}
		
	@Override
	public void setupPartitions(int numOfReducers) {
		int divisions = (numOfReducers-1)/(numSpaceParts-1);
		ArrayList<WritableChar> preds = new ArrayList<WritableChar>();
		initializePredicates(preds);
		if (divisions == 1){
			finalizeGlobalRoot(preds);
			return;
		}		
		ArrayList<Pair<HSPLeafNode<WritableChar, WritableString, R>, Integer>> lowNodes = initializeGlobalRoot(preds);
		divisions--;
		while(true){
			int most = -1;
			int j = 0;
			for (int i=0; i<lowNodes.size(); i++){
				if (lowNodes.get(i).getFirst().getKeyRecords().size() > most){
					most = lowNodes.get(i).getFirst().getKeyRecords().size();
					j = i;
				}
			}
			lowNodes.addAll(j+1,splitAndUpdate(lowNodes.get(j),preds));
			lowNodes.remove(j);
			divisions--;
			if (divisions <= 0) break;
		}
		makeReferences(lowNodes);
	}

	@Override
	public boolean range(WritableChar e, WritableString k1, WritableString k2,
			int level) {
		return false;
	}

	@Override
	public boolean range(WritableString check, WritableString key1,
			WritableString key2) {
		return false;
	}
}
