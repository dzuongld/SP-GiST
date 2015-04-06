package edu.purdue.cs.HSPGiST.UserDefinedSection;

import java.util.ArrayList;

import edu.purdue.cs.HSPGiST.AbstractClasses.HSPIndex;
import edu.purdue.cs.HSPGiST.AbstractClasses.HSPNode;
import edu.purdue.cs.HSPGiST.SupportClasses.HSPLeafNode;
import edu.purdue.cs.HSPGiST.SupportClasses.Pair;
import edu.purdue.cs.HSPGiST.SupportClasses.WritablePoint;
import edu.purdue.cs.HSPGiST.SupportClasses.WritableRectangle;

/**
 * Basic implementation of a PR quadtree
 * Has some irregular quirks but works for the most part
 * @author Stefan Brinton
 *
 */
public class QuadTree<R> extends HSPIndex<WritableRectangle,WritablePoint,R>{
	public QuadTree(){
		numSpaceParts = 4;
		resolution = 50;
		path = PathShrink.LEAF;
		nodeShrink = true;
	}
	private static final int RANGE = 1000;
	
	@Override
	public boolean picksplit(
			HSPLeafNode<WritableRectangle, WritablePoint, R> leaf, int level,
			ArrayList<ArrayList<Pair<WritablePoint, R>>> childrenData,
			ArrayList<WritableRectangle> childrenPredicates) {
		if(level == 1){
			WritableRectangle upperLeft = new WritableRectangle(-RANGE,0,RANGE,RANGE);
			WritableRectangle upperRight = new WritableRectangle(0,0,RANGE,RANGE);
			WritableRectangle lowerLeft = new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE);
			WritableRectangle lowerRight = new WritableRectangle(0,-RANGE,RANGE,RANGE);
			childrenPredicates.add(upperLeft);
			childrenPredicates.add(upperRight);
			childrenPredicates.add(lowerLeft);
			childrenPredicates.add(lowerRight);
			for(Pair<WritablePoint, R> p : leaf.getKeyRecords()){
				WritablePoint point = p.getFirst();
				if(point.getX() < 0){
					if(point.getY() < 0)
						childrenData.get(2).add(p);
					else
						childrenData.get(0).add(p);
				}
				else{
					if(point.getY()<0)
						childrenData.get(3).add(p);
					else
						childrenData.get(1).add(p);
				}
			}
			return childrenData.get(0).size() > numSpaceParts || childrenData.get(1).size() > numSpaceParts 
					|| childrenData.get(2).size() > numSpaceParts || childrenData.get(3).size() > numSpaceParts;
		}
		WritableRectangle predic = leaf.getPredicate();
		if(predic == null){
			return false;
		}
		double x = predic.getX();
		double y = predic.getY();
		double h = predic.getH();
		double w = predic.getW();
		WritableRectangle upperLeft = new WritableRectangle(x,y+h/2,h/2,w/2);
		WritableRectangle upperRight = new WritableRectangle(x+w/2,y+h/2,h/2,w/2);
		WritableRectangle lowerLeft = new WritableRectangle(x,y,h/2,w/2);
		WritableRectangle lowerRight = new WritableRectangle(x+w/2,y,h/2,w/2);
		childrenPredicates.add(upperLeft);
		childrenPredicates.add(upperRight);
		childrenPredicates.add(lowerLeft);
		childrenPredicates.add(lowerRight);
		for(Pair<WritablePoint,R> p : leaf.getKeyRecords()){
			if(upperLeft.contains(p.getFirst()))
				childrenData.get(0).add(p);
			else if(upperRight.contains(p.getFirst()))
				childrenData.get(1).add(p);
			else if(lowerLeft.contains(p.getFirst()))
				childrenData.get(2).add(p);
			else
				childrenData.get(3).add(p);
		}
		boolean test0 = childrenData.get(0).size() > numSpaceParts;
		boolean test1 = childrenData.get(1).size() > numSpaceParts;
		boolean test2 = childrenData.get(2).size() > numSpaceParts;
		boolean test3 = childrenData.get(3).size() > numSpaceParts;
		return test0 || test1 || test2 ||test3;  
	}

	@Override
	public WritableRectangle determinePredicate(WritablePoint key,
			WritableRectangle parentPred, int level) {
		WritableRectangle upperLeft;
		WritableRectangle upperRight;
		WritableRectangle lowerLeft;
		WritableRectangle lowerRight;
		if(parentPred != null){
			double x = parentPred.getX();
			double y = parentPred.getY();
			double h = parentPred.getH();
			double w = parentPred.getW();
			upperLeft = new WritableRectangle(x,y+h/2,h/2,w/2);
			upperRight = new WritableRectangle(x+w/2,y+h/2,h/2,w/2);
			lowerLeft = new WritableRectangle(x,y,h/2,w/2);
			lowerRight = new WritableRectangle(x+w/2,y,h/2,w/2);
		}
		else{
			upperLeft = new WritableRectangle(-RANGE,0,RANGE,RANGE);
			upperRight = new WritableRectangle(0,0,RANGE,RANGE);
			lowerLeft = new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE);
			lowerRight = new WritableRectangle(0,-RANGE,RANGE,RANGE);
		}
		if(upperLeft.contains(key))
			return upperLeft;
		else if(upperRight.contains(key))
			return upperRight;
		else if(lowerLeft.contains(key))
			return lowerLeft;
		else
			return lowerRight;
		
	}
	
	@Override
	public boolean consistent(HSPNode<WritableRectangle, WritablePoint,R> e,
			WritablePoint q, int level) {
		return e.getPredicate().contains(q);
	}
	
	@Override
	public boolean consistent(WritableRectangle e, WritablePoint q, int level) {
		return e.contains(q);
	}
	
	@Override
	public void setupPartitions(int numOfReducers) {
		int divisions = (numOfReducers - 1)/(numSpaceParts-1);
		WritableRectangle upperLeft = new WritableRectangle(-RANGE,0,RANGE,RANGE);
		WritableRectangle upperRight = new WritableRectangle(0,0,RANGE,RANGE);
		WritableRectangle lowerLeft = new WritableRectangle(-RANGE,-RANGE,RANGE,RANGE);
		WritableRectangle lowerRight = new WritableRectangle(0,-RANGE,RANGE,RANGE);
		ArrayList<WritableRectangle> preds = new ArrayList<WritableRectangle>();
		if(divisions == 1){
			preds.add(upperLeft);
			preds.add(upperRight);
			preds.add(lowerLeft);
			preds.add(lowerRight);
			finalizeGlobalRoot(preds);
			return;
		}
		preds.add(upperLeft);
		preds.add(upperRight);
		preds.add(lowerLeft);
		preds.add(lowerRight);
		ArrayList<Pair<HSPLeafNode<WritableRectangle, WritablePoint, R>, Integer>> lowNodes = initializeGlobalRoot(preds);
		divisions--;
		HSPLeafNode<WritableRectangle, WritablePoint, R> splitter;
		int most;
		int j;
		while(true){
			preds.clear();
			most = -1;
			j = 0;
			for(int i = 0; i < lowNodes.size(); i++){
				if(lowNodes.get(i).getFirst().getKeyRecords().size() > most){
					most = lowNodes.get(i).getFirst().getKeyRecords().size();
					j = i;
				}
			}
			
			splitter = lowNodes.get(j).getFirst();
			
			double x = splitter.getPredicate().getX();
			double y = splitter.getPredicate().getY();
			double h = splitter.getPredicate().getH();
			double w = splitter.getPredicate().getW();
			preds.add(new WritableRectangle(x,y+h/2,h/2,w/2));
			preds.add(new WritableRectangle(x+w/2,y+h/2,h/2,w/2));
			preds.add(new WritableRectangle(x,y,h/2,w/2));
			preds.add(new WritableRectangle(x+w/2,y,h/2,w/2));
			lowNodes.addAll(j+1,splitAndUpdate(lowNodes.get(j), preds));
			lowNodes.remove(j);
			divisions--;
			if(divisions <= 0){
				break;
			}
		}
		makeReferences(lowNodes);
	}

	@Override
	public boolean range(WritableRectangle e, WritablePoint k1,
			WritablePoint k2, int level) {
		WritableRectangle range = new WritableRectangle(k1.getX(), k1.getY(), k2.getY() - k1.getY(),k2.getX()-k1.getX());
		return range.overlaps(e);
	}

}