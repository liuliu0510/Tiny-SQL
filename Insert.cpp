#include "Operations.h"


void Operations::Insert(string relation_name,	vector<string> field_names,	vector<string> sv,	vector<int> iv)
{
	//cerr<<"Insert a tuple to Relation: "<<relation_name<<endl;
	Relation* rp= schema_manager.getRelation(relation_name);
	if (rp==NULL)
	{
    cerr << "No Such Relation"<<endl;
    return;
	}
	Tuple newtpl = rp->createTuple();
	int intnum=0;
	int strnum=0;
	for(int i =0;i<newtpl.getNumOfFields();i++)
	{
		if(newtpl.getSchema().getFieldType(field_names[i])==INT)
		{
			newtpl.setField(field_names[i],iv[intnum]);
			intnum++;
		}
		else
		{
			newtpl.setField(field_names[i],sv[strnum]);
			strnum++;
		}
	}
	Append_Tuple(rp,mem,0,newtpl);
}

// the appendTupleToRelation function of TestStorageManager.cpp
void Append_Tuple(Relation* rp, MainMemory& mem, int memory_block_index, Tuple& tuple)
{
  Block* bp;
  if (rp->getNumOfBlocks()==0) // relation is empty
  {
    bp=mem.getBlock(memory_block_index);
    bp->clear(); //clear the block
    bp->appendTuple(tuple); // append the tuple
    rp->setBlock(rp->getNumOfBlocks(),memory_block_index); //Write to the first block of the relation
  } 
  else //Read the last block of the relation into memory block   
  {
    rp->getBlock(rp->getNumOfBlocks()-1,memory_block_index); 
    bp=mem.getBlock(memory_block_index);
    if (bp->isFull())  // block is full: Clear the memory block and append the tuple 
    {
      bp->clear();  
      bp->appendTuple(tuple); 
      rp->setBlock(rp->getNumOfBlocks(),memory_block_index); //write to a new block at the end of the relation
    } 
    else //block is not full: Append it directly
    {
      bp->appendTuple(tuple);
      rp->setBlock(rp->getNumOfBlocks()-1,memory_block_index); //write to the last block of the relation
    }
  }  
}


