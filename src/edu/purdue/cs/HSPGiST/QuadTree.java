package edu.purdue.cs.HSPGiST;

import java.util.ArrayList;

/**
 * Basic implementation of a PR quadtree
 * Has some irregular quirks but works for the most part
 * @author Stefan Brinton
 *
 */
public class QuadTree<R> extends HSPIndex<WritableRectangle,WritablePoint,R>{
	QuadTree(){
		numSpaceParts = 4;
		resolution = 50;
		path = PathShrink.LEAF;
		nodeShrink = false;
	}
	@Override
	public boolean consistent(HSPNode<WritableRectangle, WritablePoint,R> e,
			WritablePoint q, int level) {
		return e.getPredicate().contains(q);
	}

	@Override
	public boolean picksplit(
			HSPLeafNode<WritableRectangle, WritablePoint, R> leaf, int level,
			ArrayList<ArrayList<Pair<WritablePoint, R>>> childrenData,
			ArrayList<WritableRectangle> childrenPredicates) {
		if(level == 1){
			WritableRectangle upperLeft = new WritableRectangle(-100000,0,100000,100000);
			WritableRectangle upperRight = new WritableRectangle(0,0,100000,100000);
			WritableRectangle lowerLeft = new WritableRectangle(-100000,-100000,100000,100000);
			WritableRectangle lowerRight = new WritableRectangle(0,-100000,100000,100000);
			childrenPredicates.add(upperLeft);
			childrenPredicates.add(upperRight);
			childrenPredicates.add(lowerLeft);
			childrenPredicates.add(lowerRight);
			for(Pair<WritablePoint, R> p : leaf.keys){
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
		for(Pair<WritablePoint,R> p : leaf.keys){
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
		return null;
	}
}