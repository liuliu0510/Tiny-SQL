#include "Operations.h"

vector<Tuple> Operations::Singletable_Select(string relation_name, condition con)
{
	vector<Tuple> res;
	Relation* rp = schema_manager.getRelation(relation_name);
	Block* bp;
	if (rp==NULL)
	{
       cerr << "No Such Relation"<<endl;
       return res;
	}
	for(int i=0;i<rp->getNumOfBlocks();i++)
	{
		rp->getBlock(i,0);
		bp=mem.getBlock(0);
		vector<Tuple> tuples = bp->getTuples();
	    vector<Tuple>::iterator iter;
		for(iter=tuples.begin(); iter!=tuples.end();)  
        {  
			if(con == NULL || con->judge(*iter)) //if this tuple fit the condition
			{
				res.push_back(*iter);
			}
			iter = tuples.erase(iter);
        }  
	}
	return res;
}
