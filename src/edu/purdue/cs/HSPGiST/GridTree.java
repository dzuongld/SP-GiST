package edu.purdue.cs.HSPGiST;

import java.util.ArrayList;

public class GridTree extends HSPIndex<WritableRectangle,WritablePoint>{
	GridTree(){
		numSpaceParts = 4;
		resolution = 50;
		path = PathShrink.LEAF;
		nodeShrink = false;
	}
	@Override
	public boolean consistent(HSPNode<WritableRectangle, WritablePoint> e,
			WritablePoint q, int level) {
		return e.getPredicate().contains(q);
	}

	@Override
	public boolean picksplit(
			HSPLeafNode<WritableRectangle, WritablePoint> leaf, int level,
			ArrayList<ArrayList<WritablePoint>> childrenKeys,
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
			for(WritablePoint p : leaf.keys){
				if(p.getX() < 0){
					if(p.getY() < 0)
						childrenKeys.get(2).add(p);
					else
						childrenKeys.get(0).add(p);
				}
				else{
					if(p.getY()<0)
						childrenKeys.get(3).add(p);
					else
						childrenKeys.get(1).add(p);
				}
			}
			return childrenKeys.get(0).size() == numSpaceParts || childrenKeys.get(1).size() == numSpaceParts 
					|| childrenKeys.get(2).size() == numSpaceParts || childrenKeys.get(3).size() == numSpaceParts;
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
		for(WritablePoint p : leaf.keys){
			if(upperLeft.contains(p))
				childrenKeys.get(0).add(p);
			else if(upperRight.contains(p))
				childrenKeys.get(1).add(p);
			else if(lowerLeft.contains(p))
				childrenKeys.get(2).add(p);
			else
				childrenKeys.get(3).add(p);
		}
		boolean test0 = childrenKeys.get(0).size() == numSpaceParts;
		System.out.println(test0);
		boolean test1 = childrenKeys.get(1).size() == numSpaceParts;
		System.out.println(test1);
		boolean test2 = childrenKeys.get(2).size() == numSpaceParts;
		System.out.println(test2);
		boolean test3 = childrenKeys.get(3).size() == numSpaceParts;
		System.out.println(test3);
		return test0 || test1 || test2 ||test3;  
	}

	@Override
	public WritableRectangle determinePredicate(WritablePoint key,
			WritableRectangle parentPred, int level) {
		return null;
	}

}
