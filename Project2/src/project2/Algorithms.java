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
	
	private static int hashJoinIONum = 0;
	

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
		int R_Blocks = relR.getNumBlocks();
		int S_Blocks = relS.getNumBlocks();
		int M = Setting.memorySize;
		
		//Insert your code here!
		
		System.out.println("===Starting hashJoinRelations===");
		
		System.out.println("===Starting Phase 1 of Hash Join - Hashing Relation into Buckets===");
		
		Block[][] Rbuckets = hashIntoBucket(relR);
		if (Rbuckets == null) return -1;
		
		System.out.println("----- ----- -----");
		
		Block[][] Sbuckets = hashIntoBucket(relS);
		if (Sbuckets == null) return -1;
		
		System.out.println("----- ----- -----");
		
		System.out.println("===Starting Phase 2 of Hash Join - Joining===");
		
		int Rbucket_i_Size = 0;
		int Sbucket_i_Size = 0;
		Block curBlock = new Block();
		//used to hold returned objects, which is an unfilled block,
		//and the relRS
		ArrayList<Object> arr = new ArrayList<Object>();
		for (int i=0;i<M-1;i++){
			System.out.println("+Processing Bucket "+i +" for Phase 2");
			//perform hash join precondition check
			for (int l=0;l<Rbuckets[i].length;l++){
				if (Rbuckets[i][l] != null) Rbucket_i_Size = l+1;
				else break;
			}
			
			for (int l=0;l<Sbuckets[i].length;l++){
				if (Sbuckets[i][l] != null) Sbucket_i_Size = l+1;
				else break;
			}
			//case 0 : either one of the bucket is empty
			if (Rbucket_i_Size == 0 || Sbucket_i_Size == 0){
				System.out.println("++One of Two Bucket is Empty. - Continue with next Bucket.");
				//do nothing
			}
			//case 1: both bucket can fit in main memory buffer
			else if ((Rbucket_i_Size+Sbucket_i_Size)<(M-1)){
				System.out.println("++Both Buckets can Fit in Main Memory Buffer, Block_R = "+
						Rbucket_i_Size+", Block_S = "+Sbucket_i_Size);
				//Both buckets are read into buffer
				hashJoinIONum += Rbucket_i_Size;
				hashJoinIONum += Sbucket_i_Size; 
				arr = hashJoin2Buckets(Rbuckets[i], Sbuckets[i], curBlock, relRS);
			}
			//case 2: bucket i of R is bigger than or equal to bucket i of S
			else if(Rbucket_i_Size>=Sbucket_i_Size){
				System.out.println("++Operating with Bucket i of R in Buffer, Block_R = "+
						Rbucket_i_Size+", Block_S = "+Sbucket_i_Size);
				//Bucket R is read into buffer, the blocks in bucket S is read whenever
				//the previous block has finished processing
				hashJoinIONum += Rbucket_i_Size;
				arr = hashJoin1Bucket1Block(Rbuckets[i], Sbuckets[i], curBlock, relRS);
			}
			//case 3: bucket i of S is bigger than bucket i of R
			else if(Sbucket_i_Size>Rbucket_i_Size){
				System.out.println("++Operating with Bucket i of S in Buffer, Block_R = "+
						Rbucket_i_Size+", Block_S = "+Sbucket_i_Size);
				//Bucket S is read into buffer, the blocks in bucket R is read whenever
				//the previous block has finished processing
				hashJoinIONum += Sbucket_i_Size;
				arr = hashJoin1Bucket1Block(Sbuckets[i], Rbuckets[i], curBlock, relRS);
			}
			
			System.out.println("----- ----- -----");
			
			//update returned objects
			curBlock = (Block)arr.get(0);
			relRS = (Relation)arr.get(1);
		}
		
		//write the final block to relRS
		RelationWriter writerRS = relRS.getRelationWriter();
		writerRS.writeBlock(curBlock);
		
		printFirst5Last5Tuples(relRS);
		
		System.out.println("===Finishing 1-Pass Hash Join===");
		System.out.println("+Total # of I/O = "+hashJoinIONum);
		
		return hashJoinIONum;
	}//end of hash join
	
	/**
	 * Addition method for Hash Join implementation. 
	 */
	 
	 private int hashF(int key){
		 int M = Setting.memorySize;
		 //Assumes that there is only one key, an integer
		 //Return haskKey 0 - M-2
		 return key%(M-1);
	 }

	 
	 private Block[][] hashIntoBucket(Relation relR){
		 
		int M = Setting.memorySize;
 
		System.out.println("+Beginning Hashing Relation's Tuple into Buckets");
		System.out.println("+# of Blocks in Relation = "+relR.getNumBlocks());
		 
		//Partition relR into M-1 buckets.
		RelationLoader loaderR = relR.getRelationLoader();
		//Allocate space for M-1 buckets, each bucket with at most M-2 blocks
		Block[][] Rbuckets = new Block[M-1][M-2];
		//temp blocks to hold tuples before inserting into bucket
		Block[] holdingBlock = new Block[M-1];
		for (int i=0;i<M-1;i++)
			holdingBlock[i] = new Block();
		while (loaderR.hasNextBlock()){
			Block[] Rblock = loaderR.loadNextBlocks(1);
			hashJoinIONum++;
			ArrayList<Tuple> tuples = Rblock[0].tupleLst;
			for (Tuple t : tuples){
				//Get the tuple key
				int hKey = hashF(t.key);
				//check if current block has available space
				if (!holdingBlock[hKey].insertTuple(t)){
					//check if current bucket has available space
					for (int i=0;i<(M-2);i++){
						if(Rbuckets[hKey][i]==null){
							//writing block to bucket on disk.
							Rbuckets[hKey][i] = holdingBlock[hKey];
							hashJoinIONum++;
							holdingBlock[hKey] = new Block();
							holdingBlock[hKey].insertTuple(t);
							break;
						}
						if (i == (M-3)){
							//current bucket is full, print error
							System.out.println("++Aborting Hash Join - "+
								"Bucket["+hKey+"] overflowed");
							return null;
						}
					}//end of for loop
				}
			}///end of tuple for loop
		}//end of while loop
			
		//processed all tuples in relation
		//insert all blocks into respective buckets
		for (int i=0;i<M-1;i++){
			//Check if block has tuple in it
			ArrayList<Tuple> t = holdingBlock[i].tupleLst;
			if (!t.isEmpty()){
				//check if current bucket has available space
				for (int j=0;j<(M-2);j++){
					if(Rbuckets[i][j]==null){
						//writing block to bucket on disk
						Rbuckets[i][j] = holdingBlock[i];
						hashJoinIONum++;
						break;
					}
					if (j == (M-3)){
						//current bucket is full, print error
						System.out.println("++Aborting Hash Join - "+
							"Bucket["+i+"] overflowed");
						return null;
					}
				}//end of for loop
			}
		}//end of insert block loop
		System.out.println("+Finishing Hashing Relation's Tuple into Buckets");
		System.out.println("+hashJoinIONum thusfar = "+hashJoinIONum);
		return Rbuckets;
	 }
	 
	 private ArrayList<Object> hashJoin2Buckets(Block[] Rbucket, Block[] Sbucket, 
			 Block curBlock, Relation relRS){

		 System.out.println("+++Starting hashJoin2Buckets");
		 
		 RelationWriter writerRS = relRS.getRelationWriter();
		 
		 //for each tuple in bucket R, look through every tuple in bucket S
		 //for a match key and join them.
		 for (Block bR : Rbucket){
			 if (bR == null) break;
			 ArrayList<Tuple> tuplesR = bR.tupleLst;
			 for (Tuple tR : tuplesR){
				 if (tR == null) break;
				 for (Block bS : Sbucket){
					 if (bS == null) break;
					 ArrayList<Tuple> tuplesS = bS.tupleLst;
					 for (Tuple tS : tuplesS){
						 if (tS == null) break;
						 if (tR.key==tS.key){
							 JointTuple jt = new JointTuple(tR, tS);
							 if (!curBlock.insertTuple(jt)){
								 writerRS.writeBlock(curBlock);
								 curBlock = new Block();
								 curBlock.insertTuple(jt);
							 }
						 }
					 }
				 }
			 }
		 }
		//used to return multiple object
		 ArrayList<Object> arr = new ArrayList<Object>();
		 arr.add(curBlock);
		 arr.add(relRS);
		 
		 System.out.println("+++Finishing hashJoin2Buckets");
		 System.out.println("+++hashJoinIONum thusfar = "+hashJoinIONum);
		 
		 return arr;
	 }
	 
	 private ArrayList<Object> hashJoin1Bucket1Block(Block[] fBucket, Block[] sBucket,
			 Block curBlock, Relation relRS){
		 
		 System.out.println("+++Starting hashJoin1Bucket1Block");
		 
		 RelationWriter writerRS = relRS.getRelationWriter();
		 
		 //simulate loading 1 block at a time into workingBlock for sBucket
		 int loadBlock = 0;
		 while (sBucket[loadBlock] != null){
			 hashJoinIONum++;
			 Block workingBlock = sBucket[loadBlock];
			 ArrayList<Tuple> tuplesS = workingBlock.tupleLst;
			 for (Tuple tS : tuplesS){
				 if (tS == null) break;
				 //assume that the entire fBucket is in memory memory buffer
				 for (Block bF : fBucket){
					 if (bF == null) break;
					 ArrayList<Tuple> tuplesF = bF.tupleLst;
					 for(Tuple tF : tuplesF){
						 if (tF == null) break;
						 if (tS.key==tF.key){
							 JointTuple jt = new JointTuple(tS, tF);
							 if (!curBlock.insertTuple(jt)){
								 writerRS.writeBlock(curBlock);
								 curBlock = new Block();
								 curBlock.insertTuple(jt);
							 }
						 }
					 }
				 }
			 }
			 loadBlock++;
		 }
		//used to return multiple object
		 ArrayList<Object> arr = new ArrayList<Object>();
		 arr.add(curBlock);
		 arr.add(relRS);
		 
		 System.out.println("+++Finishing hashJoin1Bucket1Block");
		 System.out.println("+++hashJoinIONum thusfar = "+hashJoinIONum);
		 
		 return arr;
	 }
	 
	 private void printFirst5Last5Tuples(Relation rel){
		 
		 //only work correctly if key goes from 0 - 999
		 
		 //hold the 1000 buckets
		 ArrayList<ArrayList<Tuple>> buckets = new ArrayList<ArrayList<Tuple>>();
		 for (int i=0;i<1000;i++){
			 buckets.add(new ArrayList<Tuple>());
		 }
		 
		 RelationLoader loader = rel.getRelationLoader();
		 while (loader.hasNextBlock()){
			 Block block = loader.loadNextBlocks(1)[0];
			 ArrayList<Tuple> tuples = block.tupleLst;
			 for (Tuple t : tuples){
				 int hkey = t.key%1000;
				 buckets.get(hkey).add(t);
			 }
		 }
		 
		 //print total number of tuples
		 System.out.println("Total # of tuples: "+rel.getNumTuples());
		 
		 int count = 0;
		 //print the first 5 tuples
		 for (int i=0;i<1000;i++){
			 if (buckets.get(i)!=null){
				 if (buckets.get(i).size() == 0) continue;
				 for (int j=0;j<buckets.get(i).size();j++){
					 System.out.println("Tuple #" + (count+1) + ": " + 
						 buckets.get(i).get(j).toString());
					 count++;
					 if (count == 5) break;
				 }
			 }
			 if (count == 5) break;
		 }
		 
		 count = 0;
		 //print the last 5 tuples
		 for (int i=999;i>-1;i--){
			 if (buckets.get(i)!=null){
				 if (buckets.get(i).size()==0) continue; 
				 for (int j=buckets.get(i).size()-1;j>-1;j--){
					 System.out.println("Last Tuple #" + (count+1) + ": " + 
						 buckets.get(i).get(j).toString());
					 count++;
					 if (count == 5) break;
				 }
			 }
			 if (count == 5) break;
		 }
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
		//Algorithms.testCases();
		Relation relR=new Relation("RelR");
		int numTuples=relR.populateRelationFromFile("RelR.txt");
		Relation relS=new Relation("RelS");
		numTuples=relS.populateRelationFromFile("RelS.txt");
		
		Algorithms algo = new Algorithms();
		algo.hashJoinRelations(relR, relS, new Relation("test"));
	}
}
