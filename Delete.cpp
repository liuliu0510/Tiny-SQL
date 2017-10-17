#include "Operations.h"


void Operations::Delete(string relation_name, condition con)  //using mem 0 to get the block, mem1 to store the tuples written back
{
	Relation* rp = schema_manager.getRelation(relation_name);
	if (rp==NULL)
	{
       cerr << "No Such Relation"<<endl;
	   return;
	}
	int tuplesBack = 0;
	int blockback=0;
	cerr<<"Delete"<<endl;
	Block* bp0=mem.getBlock(0); 
	bp0->clear();
	Block* bp1=mem.getBlock(1);
	bp1->clear();
	
	Tuple tuple = rp->createTuple();

	for(int i=0;i< rp->getNumOfBlocks();i++)
	{
		rp->getBlock(i,0);
		int tuplesnum = bp0->getNumTuples();
		for(int j=0;j<tuplesnum;j++)
		{
			if(! ( con == NULL || con->judge(bp0->getTuple(j) ) ) )
			{
				bp1->setTuple(tuplesBack,bp0->getTuple(j));
				tuplesBack++;
				if(tuplesBack==rp->getSchema().getTuplesPerBlock())
				{
					rp->setBlock(blockback,1);
					tuplesBack=0;
					blockback++;
				}
			}
		}
	}
	rp->deleteBlocks(blockback);
	return;
}