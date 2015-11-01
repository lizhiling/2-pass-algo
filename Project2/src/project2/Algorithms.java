/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;

import junit.framework.Assert;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

public class Algorithms {
	

	public static Comparator<Tuple> TupleComparator = new Comparator<Tuple>() {

		public int compare(Tuple t1, Tuple t2) {
			int k1 = t1.key;
			int k2 = t2.key;

			// ascending order
			return k1-k2;
		}
	};
	
	/**
	 * Sort the relation using Setting.memorySize buffers of memory 
	 * @param rel is the relation to be sorted. 
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int mergeSortRelation(Relation rel){
		int numIO=0;
		ArrayList<Block[]> sublists = new ArrayList<Block[]>();
		System.out.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader loader=rel.getRelationLoader();	
		loader.reset();
		while(loader.hasNextBlock()){
			//load M blocks
			//System.out.println("--->Load at most "+Setting.memorySize+" blocks each time into memory...");
			Block[] blocks=loader.loadNextBlocks(Setting.memorySize);
			numIO += blocks.length;
			LinkedList<Tuple> sublist = new LinkedList<Tuple>();
			for(Block b:blocks){
				if(b!=null){
					sublist.addAll(b.tupleLst);
				}
			}
			//sort them
			sublist.sort(TupleComparator);
			//insert sorted tuples to new M blocks
			int i = 0;
			Block[] sortedBlocks = new Block[blocks.length];
			for(int j = 0; j < blocks.length; j++){
				sortedBlocks[j] = new Block();
			}
			Block thisBlock = sortedBlocks[i];
			while(!sublist.isEmpty()){
				if(thisBlock.getNumTuples()>=Setting.blockFactor){
					//change to next empty block
					++i;
					thisBlock = sortedBlocks[i];
				}
				thisBlock.insertTuple(sublist.get(0));
				sublist.remove(0);
			}
			numIO += sortedBlocks.length;
			sublists.add(sortedBlocks);
		}
		System.out.println("---------Finish sort of sublist----------\n\n");
		
		//load again
		int[] index = new int[sublists.size()];
		for(int i = 0; i<index.length;i++){
			index[i] = 1;
		}
		Block[] firstblocks = new Block[sublists.size()];
		Block outBuffer = new Block();
		int filledBlock = 0;
		
		//load every first block of each sublist first
		for(int i = 0 ; i < sublists.size(); i++){
			firstblocks[i] = sublists.get(i)[0];
		}
		RelationWriter sWriter=rel.getRelationWriter();
		while(!sublists.isEmpty()){
			int minimumKey = 99999;
			int minimumTupleIndex = 0;
			for(int i = 0 ; i < firstblocks.length; i++){
				Block b = firstblocks[i];
				if(b==null || b.tupleLst.isEmpty())
					continue;
				Tuple firstTuple = b.tupleLst.get(0);
				if(firstTuple.key <= minimumKey){
					minimumKey = firstTuple.key;
					minimumTupleIndex = i;
				}
			}
			//remove minimum tuple of firstblocks
			Block targetBlock = firstblocks[minimumTupleIndex];
			if(targetBlock.tupleLst.isEmpty()){
				sWriter.writeBlock(outBuffer, filledBlock);
				filledBlock++;
				numIO++;
				break;
			}
			//add this tuple to output buffer memory
			//if output buffer is full, move this block to disk
			if(outBuffer.getNumTuples()==Setting.blockFactor){
				sWriter.writeBlock(outBuffer, filledBlock);
				outBuffer = new Block();
				filledBlock++;
				numIO++;
			}	
			outBuffer.insertTuple(targetBlock.tupleLst.get(0));
			targetBlock.tupleLst.remove(0);
			if(targetBlock.tupleLst.isEmpty()){
				if(index[minimumTupleIndex]>=sublists.get(minimumTupleIndex).length){
					sublists.set(minimumTupleIndex, null);
					continue;
				}
				firstblocks[minimumTupleIndex] = sublists.get(minimumTupleIndex)[index[minimumTupleIndex]];
				index[minimumTupleIndex]++;
			}
		}
		
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		//Insert your code here!
		
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	
	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		//Insert your code here!
		
		return numIO;
	}

	
	
	/**
	 * Example usage of classes. 
	 */
	public static void examples(){

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR=new Relation("RelR");
		int numTuples=relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples+" tuples.");
		Relation relS=new Relation("RelS");
		numTuples=relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains "+numTuples+" tuples.");
		System.out.println("---------Finish populating relations----------\n\n");
			
		/*Print the relation */
		System.out.println("---------Printing relations----------");
		relR.printRelation(true, true);
		relS.printRelation(true, false);
		System.out.println("---------Finish printing relations----------\n\n");
		
		
		/*Example use of RelationLoader*/
		System.out.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader rLoader=relR.getRelationLoader();		
		while(rLoader.hasNextBlock()){
			System.out.println("--->Load at most 7 blocks each time into memory...");
			Block[] blocks=rLoader.loadNextBlocks(7);
			//print out loaded blocks 
			for(Block b:blocks){
				if(b!=null) b.print(false);
			}
		}
		System.out.println("---------Finish loading relation RelR----------\n\n");
				
		
		/*Example use of RelationWriter*/
		System.out.println("---------Writing to relation RelS----------");
		RelationWriter sWriter=relS.getRelationWriter();
		rLoader.reset();
		if(rLoader.hasNextBlock()){
			System.out.println("Writing the first 7 blocks from RelR to RelS");
			System.out.println("--------Before writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
			
			Block[] blocks=rLoader.loadNextBlocks(7);
			for(Block b:blocks){
				if(b!=null) sWriter.writeBlock(b);
			}
			System.out.println("--------After writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
		}

	}
	
	/**
	 * Testing cases. 
	 */
	
	public static void testCases(){
		Algorithms alg = new Algorithms();
		
		System.out.println("=============Test Relation R=======================");
		Relation relR = new Relation("RelR");
		relR.populateRelationFromFile("RelR.txt");
		System.out.println("----------start merge sort----------");
		int io = alg.mergeSortRelation(relR);
		System.out.println("Number of IO: "+io);
		//relR.printRelation(true, true);
		Assert.assertEquals("Actual IO "+io, relR.getNumBlocks()*3, io);
		
		System.out.println("=============Test Relation S=======================");
		Relation relS = new Relation("RelS");
		relS.populateRelationFromFile("RelS.txt");
		System.out.println("----------start merge sort----------");
		io = alg.mergeSortRelation(relS);
		System.out.println("Number of IO: "+io);
		//relS.printRelation(true, true);
		Assert.assertEquals("Actual IO "+io, relS.getNumBlocks()*3, io);
		
//		System.out.println("---------Test Relation S----------");
//		Relation relS=new Relation("RelS");
//		numTuples=relR.populateRelationFromFile("RelS.txt");
//		System.out.println("Relation RelS contains "+numTuples+" tuples.");
//		io = alg.mergeSortRelation(relS);
//		Assert.assertTrue(io == relS.getNumBlocks()*3);
	
	}
	
	/**
	 * This main method provided for testing purpose
	 * @param arg
	 */
	public static void main(String[] arg){
		Algorithms.testCases();

	}
}
