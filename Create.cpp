#include "Operations.h"


Relation * Operations::Create_Table(string relation_name,vector<Tuple> tuples)
{
	Schema schema=tuples.back().getSchema();
	Relation* rp=schema_manager.createRelation(relation_name,schema);
	cerr << "Table created: " << relation_name << endl ;
	
	Block * bp=mem.getBlock(0);
	bp->clear();
	int offset=0;
	int blocksnum=0;
	int tuplesnum_perblock=schema.getTuplesPerBlock();
	while(!tuples.empty())
	{
		vector<string> field_names=schema.getFieldNames();
		vector<string> sv;
		vector<int> iv;
		Tuple tpl=tuples.back();
		for(int i=0;i<schema.getNumOfFields();i++)
		{
			if(schema.getFieldType(i)==INT)
			    iv.push_back(tpl.getField(i).integer);
			else 
			    sv.push_back(*tpl.getField(i).str);
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
		bp->setTuple(offset++,newtpl);
		if(offset==tuplesnum_perblock)
		{
			rp->setBlock(blocksnum++,0);
			bp->clear();
			offset=0;
		}
		tuples.pop_back();
	}
	if(offset>0){rp->setBlock(blocksnum++,0);}
	return rp;
}

Relation * Operations::Create_Table(string relation_name, vector<string> & field_names, vector <enum FIELD_TYPE> & field_types)
{
	Schema schema(field_names,field_types);
	Relation* rp=schema_manager.createRelation(relation_name,schema);
	cerr << "Table created: " << relation_name << endl ;
	return rp;
}