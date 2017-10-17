#ifndef _DATA_STRUCTURES_H_                   // include file only once
#define _DATA_STRUCTURES_H_
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <string>
#include <iterator>
#include <vector>
#include "MainMemory.h"
#include "Config.h"
#include "SchemaManager.h"
#include "Schema.h"
#include "Disk.h"
#include "Block.h"
#include "Field.h"
#include "Relation.h"
#include "Tuple.h"
#include "common.h"
using namespace std;
typedef QueryExp* (condition) ;
class TupleAddr{                   //location if a tuple in mem
public:
	int block_index;
	int offset;
	TupleAddr()
	{
		block_index=0;
		offset=0;
	}
	TupleAddr(int _block_index,int _offset)
	{
		block_index=_block_index;
		offset=_offset;
	}
};

class relation_data{
public:
	string relation_name;
	Relation* relation_ptr;
    int numT;
	vector<int> Vec;     //in the order of field names' order
	Schema schema;
	
	relation_data(vector<int> v,int t,string name,Schema _schema,Relation * ptr=NULL)
	{
		Vec=v;numT=t;relation_name=name; relation_ptr=ptr;schema=_schema;
	}
	relation_data(){
		numT=0;relation_name=""; relation_ptr=NULL;
	}
	void print()
	{
		cerr<<"Relation : "<<relation_name<<" has "<<numT<<" tuples, and Vec is :"<<endl;
		for(int i = 0; i<schema.getNumOfFields();i++)
		{
			cerr<<schema.getFieldName(i)+"  ";
		}
		cerr<<endl;
		for(int i = 0; i<schema.getNumOfFields();i++)
		{
			cerr<<Vec[i]<<"  ";
		}
		cerr<<endl;
	}
};
class JoinNode
{
public:
	JoinNode *left;
	JoinNode *right;
	relation_data m;

	JoinNode(relation_data _m,JoinNode *_l=NULL,JoinNode *_r=NULL)
	{
		m=_m;left=_l;right=_r;
	}
	JoinNode()
	{
		left=NULL;right=NULL;
	}
	void print()
	{
		cerr<<m.relation_name<<" : "<<m.numT<<endl;
		if(left!=NULL)left->print();
		if(right!=NULL)right->print();
	}




};

#endif
