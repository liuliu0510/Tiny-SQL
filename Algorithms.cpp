#include "Operations.h"


vector<Tuple> Operations::Sort_In_Memory(string field_name,int startpos, int blocksnum)
{
	Block *bp;
	bp=mem.getBlock(startpos);
	Schema schema=bp->getTuple(0).getSchema();
	vector<Tuple> tuples,res;
	int i,j;
	for(i=startpos;i<startpos+blocksnum;i++)
	{
		bp=mem.getBlock(i);
		for(j=0;j<bp->getNumTuples();j++)
		{
			Tuple tup=bp->getTuple(j);
			if(tup.getSchema()!=schema)
			  {
			  	cerr<<"Different schema!"<<endl;
			  	return res;
			  }
			tuples.push_back(tup);
		}
	}
    //the sort process 
	for(i=0;i<tuples.size()-1;i++)
	{
		for(j = 0;j<tuples.size()-1-i;j++)
		{
			if(schema.getFieldType(field_name)==STR20)
			{
				if(*tuples[j].getField(field_name).str<*tuples[j+1].getField(field_name).str)
				{
					Tuple tmp=tuples[j];
					tuples[j]=tuples[j+1];
					tuples[j+1]=tmp;
				}
			}
			else if(schema.getFieldType(field_name)==INT)
			{
				if(tuples[j].getField(field_name).integer<tuples[j+1].getField(field_name).integer)
				{
					Tuple tmp=tuples[j];
					tuples[j]=tuples[j+1];
					tuples[j+1]=tmp;
				}
			}
		}
	}
	for(i=startpos;i<startpos+blocksnum;i++)
	{
		bp=mem.getBlock(i);
		for(j=0;j<schema.getTuplesPerBlock();j++)
		{
			if(tuples.empty()) break;
			Tuple tup=tuples.back();
			res.push_back(tup);
			tuples.pop_back();
			bp->setTuple(j,tup);
		}
	}
	return res;
}

vector<Tuple> Operations::OnePass_Sort(string relation_name,string field_name)
{
	Relation* rp = schema_manager.getRelation(relation_name);
	vector<Tuple> res;
	if(rp->getNumOfBlocks()>10) 
	{
		cerr<<"Blocks size is too large, can't use one-pass algorithm! "<<endl;
		return res;
		
	}
	rp->getBlocks(0,0,rp->getNumOfBlocks());
	return Sort_In_Memory(field_name,0,rp->getNumOfBlocks());
}

vector<Tuple> Operations::TwoPass_Sort(string relation_name,string field_name)
{
	Relation* rp = schema_manager.getRelation(relation_name);
	Block *bp;
	vector<Tuple> res;
	TupleAddr tmp;
	Relation* sublist_p;
	if(rp->getNumOfBlocks()<=10)
	{
		cerr<<"Blocks size is small, can use the one-pass algorithm! "<<endl;
		return OnePass_Sort(relation_name,field_name);
		
	}
	vector<string> sublist=Get_sublists(relation_name,field_name);  //get sublists
	int *blocks = new int[sublist.size()];    //record blocks of each sublist having been written to memory
	int tuplesnum=rp->getNumOfTuples();
	for(int i=0;i<sublist.size();i++)       //put first block of each sublist to memory
	{
		sublist_p = schema_manager.getRelation(sublist[i]);
		sublist_p->getBlock(0,i);
		blocks[i]=0;
	}
	while(tuplesnum>0) // 
	{
		tmp=Get_Min(field_name,0,sublist.size()); 
		bp=mem.getBlock(tmp.block_index);
		res.push_back(bp->getTuple(tmp.offset));
		tuplesnum--;
		bp->nullTuple(tmp.offset);
		if(bp->getNumTuples()==0) 
		{
			sublist_p = schema_manager.getRelation(sublist[tmp.block_index]);
			if(blocks[tmp.block_index]+1<sublist_p->getNumOfBlocks())
			{
				blocks[tmp.block_index]=blocks[tmp.block_index]+1;
				sublist_p->getBlock(blocks[tmp.block_index],tmp.block_index);
			}
		}
	}
	return res;
}

//phase 1:making sorted sublists, breaking relations into smaller pieces that fit in the memory
vector<string> Operations::Get_sublists(string relation_name,string field_name)
{
	Relation* rp = schema_manager.getRelation(relation_name);
	vector<string> res;
	Schema schema=rp->getSchema();
	int totalnum = rp->getNumOfBlocks();
	int count=0;
	int sublist_num=0;
	while(totalnum>0)
	{
		int blocksnum=(totalnum<10)?totalnum:10;
		rp->getBlocks(count,0,blocksnum);
		Sort_In_Memory(field_name,0,blocksnum);
		totalnum-=blocksnum;
		char suffix='0'+ sublist_num;
		string name=relation_name+'-'+suffix;
		if(schema_manager.relationExists(name)) 
		   Drop_Table(name);
		Relation* sublist=schema_manager.createRelation(name,schema);
		sublist->setBlocks(0,0,blocksnum);
		res.push_back(name);
		sublist_num++;
		count+=blocksnum;
	}
	return res;
}

//always return the minimum part of those sublists
TupleAddr  Operations::Get_Min(string field_name,int startpos,int num_blocks)
{
	TupleAddr res;
	Block *bp;
	//read in the next block from the sublist, if the block is exhausted
	for(int i=startpos;i<startpos+num_blocks;i++)
	{
		if(!mem.getBlock(i)->isEmpty())
		{
			bp=mem.getBlock(i);
			break;
		}
	}
	vector<Tuple> tuples=bp->getTuples();
	int tuplesnum_perblock=tuples[0].getSchema().getTuplesPerBlock() ;
	//return the minimum value of the block 
	if(tuples[0].getSchema().getFieldType(field_name)==INT)
	{
		int min_integer=INT_MAX;
		res.block_index=startpos;
		res.offset=0;
		for(int i=startpos;i<startpos+num_blocks;i++)
		{
			bp=mem.getBlock(i);
			for(int j=0;j<tuplesnum_perblock;j++)
			{
				if(bp->getTuple(j).isNull()) continue;
				if(min_integer>bp->getTuple(j).getField(field_name).integer)
				{
					min_integer=bp->getTuple(j).getField(field_name).integer;
					res.block_index=i;
					res.offset=j;
				}
			}
		}
		return res;
	}
	else
	{
		//char b[1]=(char)INT_MAX;
	    char b[1];
		char a=(char)INT_MAX;
		b[0]=a;
		string min_string = string(b,1);
		res.block_index=startpos;
		res.offset=0;
		for(int i=startpos;i<startpos+num_blocks;i++)
		{
			bp=mem.getBlock(i);
			for(int j=0;j<tuplesnum_perblock;j++)
			{
				if(bp->getTuple(j).isNull()) continue;
				if(min_string>*bp->getTuple(j).getField(field_name).str)
				{
					min_string=*bp->getTuple(j).getField(field_name).str;
					res.block_index=i;
					res.offset=j;
				}
			}
		}
		return res;
	}
}

vector<Tuple> Operations::OnePass_DupElimination(string relation_name)
{
	Relation* rp = schema_manager.getRelation(relation_name);
	vector<Tuple> res;
	Block *bp;
	int blocksnum=rp->getNumOfBlocks();
	if(blocksnum>10)
	{
		cerr<<"Blocks size is too large, can't use one-pass algorithm!  "<<endl;
		return res;
	}
	rp->getBlocks(0,0,blocksnum);
	Schema schema=rp->getSchema();
	int tuplesnum_perblock=schema.getTuplesPerBlock();
	for(int i=0;i<blocksnum;i++)
	{
		bp=mem.getBlock(i);
		for(int j=0;j<tuplesnum_perblock;j++)
		{
			if(bp->getTuple(j).isNull())continue;
			vector<TupleAddr*> addr=Get_Dups(bp->getTuple(j),i,blocksnum-i);
			while(!addr.empty())
			{
				TupleAddr * ap=addr.back();
				if(ap->block_index!=i||ap->offset!=j) 
			     	mem.getBlock(ap->block_index)->nullTuple(ap->offset); //not itself,eliminate
				addr.pop_back();
			}
		}
	}
	return mem.getTuples(0,rp->getNumOfBlocks());
}

vector<Tuple> Operations::TwoPass_DupElimination(string relation_name)
{
	Relation* rp = schema_manager.getRelation(relation_name);
	vector<Tuple> res;
	Block *bp;
	string field_name = rp->getSchema().getFieldName(0);
	if(rp->getNumOfBlocks()<=10)
	{
		cerr<<"This duplicate elimination can be in one pass! "<<endl;
		return OnePass_DupElimination(relation_name);
		
	}
	vector<string> sublist=Get_sublists(relation_name,field_name);
	Relation* sublist_p;
	TupleAddr tmp;
	int *blocks = new int[sublist.size()];    
	int tuplesnum=rp->getNumOfTuples();
	for(int i=0;i<sublist.size();i++)                        //put first block of each sublist to mem
	{
		sublist_p = schema_manager.getRelation(sublist[i]);
		sublist_p->getBlock(0,i);
		blocks[i]=0;
	}
	while(tuplesnum>0)
	{
		tmp=Get_Min(field_name,0,sublist.size());
		bp=mem.getBlock(tmp.block_index);
		res.push_back(bp->getTuple(tmp.offset));
		Tuple min_tuple=bp->getTuple(tmp.offset);
		vector<TupleAddr*> addr=Get_Dups(min_tuple,0,sublist.size());
		while(!addr.empty())
		{
			TupleAddr * ap=addr.back();
			bp=mem.getBlock(ap->block_index);
			bp->nullTuple(ap->offset); //eliminate
			tuplesnum--;
			addr.pop_back();
			if(bp->getNumTuples()==0)  //if this block of sublist is empty, put next block of this sublist
			{
				sublist_p = schema_manager.getRelation(sublist[ap->block_index]);
				if(blocks[ap->block_index]+1<sublist_p->getNumOfBlocks())
				{
					blocks[ap->block_index]=blocks[ap->block_index]+1;
					sublist_p->getBlock(blocks[ap->block_index],ap->block_index);
					addr=Get_Dups(min_tuple,0,sublist.size());
				}
			}
		}
	}
	return res;
}

bool Operations::Same_Tuple(Tuple t1,Tuple t2)
{
	if(t1.isNull()||t2.isNull()||(t1.getSchema()!=t2.getSchema())) 
	   return false;
	Schema schema=t1.getSchema();
	for(int i=0;i<schema.getNumOfFields();i++)
	{
		if(schema.getFieldType(i)==INT)
		{
			if(t1.getField(i).integer!=t2.getField(i).integer) 
			  return false;	
		}
		if(schema.getFieldType(i)==STR20)
		{
			if(*t1.getField(i).str!=*t2.getField(i).str) 
			  return false;	
		}
	}
	return true;
}

//return the duplicates 
vector<TupleAddr*> Operations::Get_Dups(Tuple t,int startpos,int blocksnum)
{
	vector<TupleAddr*> res;
	Block *bp;
	for(int i=startpos;i<startpos+blocksnum;i++)
	{
		if(!mem.getBlock(i)->isEmpty())
		{
			bp=mem.getBlock(i);
			break;
		}
	}
	vector<Tuple> tuples=bp->getTuples();
	int tuplesnum_perblock =tuples[0].getSchema().getTuplesPerBlock();
	for(int i=startpos;i<startpos+blocksnum;i++)
	{
		if(!mem.getBlock(i)->isEmpty())
		   bp=mem.getBlock(i);
		else continue;
		for(int j=0;j<tuplesnum_perblock;j++)
		{
			if(Same_Tuple(bp->getTuple(j),t)) 
			   res.push_back(new TupleAddr(i,j));
		}
	}
	return res;
}

//natural join, using one-pass algorithm
vector<Tuple>  Operations::OnePass_Join(string relation_name1, string relation_name2)
{
	Relation* rp1 = schema_manager.getRelation(relation_name1);
	Relation* rp2 = schema_manager.getRelation(relation_name2);
	vector<Tuple> res;
	if (rp1==NULL||rp2==NULL)
	{
        cerr << "No Such Relation"<<endl;
		return res;
	}
	Block* bp0,*bp;
	bp0=mem.getBlock(0);
	bp0->clear();
	Relation* Rlarge=(rp1->getNumOfBlocks()>rp2->getNumOfBlocks())?rp1:rp2;
    Relation* Rsmall=(rp1->getNumOfBlocks()>rp2->getNumOfBlocks())?rp2:rp1;
	if(!Rsmall->getBlocks(0,1,Rsmall->getNumOfBlocks()))
	{
		cerr << "Blocks size is too large, can't use one-pass algorithm! "<<endl;
		return res;
	}
	for(int i=0;i<Rlarge->getNumOfBlocks();i++)
	{
		Rlarge->getBlock(i,0);
		for(int j=0;j<bp0->getNumTuples();j++)
		{
			Tuple Tlarge=bp0->getTuple(j);
			for(int k=1;k<1+Rsmall->getNumOfBlocks();k++)
			{
				bp=mem.getBlock(k);
				for(int u=0;u<bp->getNumTuples();u++)
				{
					Tuple Tsmall=bp->getTuple(u);
					Tuple t=Join_Tuple(Tsmall,Tlarge);
					if(!t.isNull())
					  res.push_back(t);	
				}
			}
		}
	}
	return res;
}
//natural join, using one-pass algorithm
vector<Tuple> Operations::OnePass_Join(string relation_name1, string relation_name2,vector<string> common_fd) 
{
	Relation* rp1 = schema_manager.getRelation(relation_name1);
	Relation* rp2 = schema_manager.getRelation(relation_name2);
	vector<Tuple> res;
	if (rp1==NULL||rp2==NULL)
	{
        cerr << "No Such Relation"<<endl;
		return res;
	}
	Block* bp0,*bp;
	bp0=mem.getBlock(0); 
	bp0->clear();
	Relation* Rlarge=(rp1->getNumOfBlocks()>rp2->getNumOfBlocks())?rp1:rp2;
	Relation* Rsmall=(rp1->getNumOfBlocks()>rp2->getNumOfBlocks())?rp2:rp1;
	if(!Rsmall->getBlocks(0,1,Rsmall->getNumOfBlocks()))
	{
		cerr << "Blocks size is too large, can't use one-pass algorithm!"<<endl;
		return res;
	}
	for(int i=0;i<Rlarge->getNumOfBlocks();i++)
	{
		Rlarge->getBlock(i,0);
		for(int j=0;j<bp0->getNumTuples();j++)
		{
			Tuple Tlarge=bp0->getTuple(j);
			for(int k=1;k<1+Rsmall->getNumOfBlocks();k++)
			{
				bp=mem.getBlock(k);
				for(int l=0;l<bp->getNumTuples();l++)
				{
					Tuple Tsmall=bp->getTuple(l);
					Tuple t=Join_Tuple(Rsmall->getRelationName(),Rlarge->getRelationName(),Tsmall,Tlarge,common_fd);
					if(!t.isNull())
					   res.push_back(t);	
				}
			}
		}
	}
	return res;
}
//natural join, using two-pass algorithm
vector<Tuple>  Operations::TwoPass_Join(string relation_name1, string relation_name2)
{
	Relation* rp1 = schema_manager.getRelation(relation_name1);
	Relation* rp2 = schema_manager.getRelation(relation_name2);
	vector<Tuple> res;
	if (rp1==NULL||rp2==NULL){
        cerr << "No Such Relation"<<endl;
	    return res;
	}
	Schema sch1 =  rp1->getSchema();
	Schema sch2 =  rp2->getSchema();
	vector<string> common_fd;   
	for(int i=0;i<sch1.getFieldNames().size();i++)
	{
		if(sch2.fieldNameExists(sch1.getFieldName(i)))
		{
			common_fd.push_back(sch1.getFieldName(i));
			continue;
		}
	}
	if(common_fd.empty())
	{
	   cerr<<"NO common fields, need to do Cross Join.";
	   return Product(relation_name1,relation_name2);
	}
	string field_name = common_fd.back();  
	if(rp1->getNumOfBlocks()<10||rp2->getNumOfBlocks()<10) 
	{
	    cerr<<"Blocks number is small, this Join can use one-pass algorithm! "<<endl;
    	return OnePass_Join(relation_name1,relation_name2);
	}
    int tuplesnum_perblock1 =sch1.getTuplesPerBlock();
	int tuplesnum_perblock2 =sch2.getTuplesPerBlock();
	vector<string> sublist1=Get_sublists(relation_name1,field_name);
	vector<string> sublist2=Get_sublists(relation_name2,field_name);
	Relation* sublist_p1,* sublist_p2;
	Block* bp1, *bp2;

	int *blocks1 = new int[sublist1.size()];  //record the number of blocks of each sublist having been written to memory
	int sum1=rp1->getNumOfTuples();
	int *blocks2 = new int[sublist2.size()];             
	int sum2=rp2->getNumOfTuples();
    int i=0;
	for(i=0;i<sublist1.size();i++)  //put first block of each sublist of relation 1 to memory
	{
		sublist_p1 = schema_manager.getRelation(sublist1[i]);
		sublist_p1->getBlock(0,i);
		blocks1[i]=0;
	}
	for(int j=0;i<sublist1.size()+sublist2.size();i++,j++) //put first block of each sublist of relation 2 to memory
	{
		sublist_p2 = schema_manager.getRelation(sublist2[j]);
		sublist_p2->getBlock(0,i);
		blocks2[j]=0;
	}
	while(sum1>0&&sum2>0)
	{
		TupleAddr minsub1=Get_Min(field_name,0,sublist1.size());
		TupleAddr minsub2=Get_Min(field_name,sublist1.size(),sublist2.size());
		bp1=mem.getBlock(minsub1.block_index);
		bp2=mem.getBlock(minsub2.block_index);
		Tuple minsub1_t=bp1->getTuple(minsub1.offset);
		Tuple minsub2_t=bp2->getTuple(minsub2.offset);
		if(Join_Tuple(minsub1_t,minsub2_t).isNull())   //can't join
		{    
			if(!Larger_Field(minsub1_t,minsub2_t,field_name,field_name)) 
			{
			    bp1->nullTuple(minsub1.offset);
				sum1--;
				if(bp1->getNumTuples()==0)       
				{
					sublist_p1 = schema_manager.getRelation(sublist1[minsub1.block_index]);
					if(blocks1[minsub1.block_index]+1<sublist_p1->getNumOfBlocks())
					{
						blocks1[minsub1.block_index]=blocks1[minsub1.block_index]+1;
						sublist_p1->getBlock(blocks1[minsub1.block_index],minsub1.block_index);
					}
				}
			}
			else 
			{
			    bp2->nullTuple(minsub2.offset);
				sum2--;
				if(bp2->getNumTuples()==0)     //if this block of sublist is empty, put next block of this sublist
				{
					int sub2_index=minsub2.block_index-sublist1.size();
					sublist_p2 = schema_manager.getRelation(sublist2[sub2_index]);
					if(blocks2[sub2_index]+1<sublist_p2->getNumOfBlocks())
					{
						blocks2[sub2_index]=blocks2[sub2_index]+1;
						sublist_p2->getBlock(blocks2[sub2_index],minsub2.block_index);
					}
				}
			}
		}
		else  //can join
		{
			vector<Tuple> mintuples1,mintuples2;
			for(i=0;i<sublist1.size();i++)
			{
				bp1=mem.getBlock(i);
				for(int j=0;j<tuplesnum_perblock1;j++)
				{
					if(bp1->getTuple(j).isNull())continue;
					if(!Join_Tuple(minsub2_t,bp1->getTuple(j)).isNull())
					{
						mintuples1.push_back(bp1->getTuple(j));
						bp1->nullTuple(j);
						sum1--;
						if(bp1->getNumTuples()==0)      //if this block of sublist is empty, put next block of this sublist
						{
							sublist_p1 = schema_manager.getRelation(sublist1[i]);
							if(blocks1[i]+1<sublist_p1->getNumOfBlocks())
							{
								blocks1[i]=blocks1[i]+1;
								sublist_p1->getBlock(blocks1[i],i);
							}
							j=-1;  
						}
					}
				}
			}
			for(int k=0;i<sublist1.size()+sublist2.size();i++,k++)
			{
				bp2=mem.getBlock(i);
				for(int j=0;j<tuplesnum_perblock2;j++)
				{
					if(bp2->getTuple(j).isNull())continue;
					if(!Join_Tuple(minsub1_t,bp2->getTuple(j)).isNull())
					{
						mintuples2.push_back(bp2->getTuple(j));
						bp2->nullTuple(j);
						sum2--;
						if(bp2->getNumTuples()==0)       
						{
							sublist_p2 = schema_manager.getRelation(sublist2[k]);
							if(blocks2[k]+1<sublist_p2->getNumOfBlocks())
							{
								blocks2[k]=blocks2[k]+1;
								sublist_p2->getBlock(blocks2[k],i);
							}
							j=-1;
						}
					}
				}
			}
			while(!mintuples1.empty())
			{
				for(int r=0;r<mintuples2.size();r++)
					res.push_back(Join_Tuple(mintuples1.back(),mintuples2[r]));
				mintuples1.pop_back();
			}
		}
	}
	return res;
}
//natural join, using two-pass algorithm
vector<Tuple> Operations::TwoPass_Join(string relation_name1,string relation_name2,vector<string> common_fd)
{
	Relation* rp1 = schema_manager.getRelation(relation_name1);
	Relation* rp2 = schema_manager.getRelation(relation_name2);
	vector<Tuple> res;
	if (rp1==NULL||rp2==NULL)
	{
        cerr << "No Such Relation"<<endl;
		return res;
	}
	Schema sch1 =  rp1->getSchema();
	Schema sch2 =  rp2->getSchema();
	vector<string> field_names1=sch1.getFieldNames();
	vector<string> field_names2=sch2.getFieldNames();
	if(common_fd.empty()) 
	{
	    cerr<<"NO common fields, need to do cross join.";
	    return Product(relation_name1,relation_name2);
	}
	int k=0;
	string field_name1 = common_fd[k++];
	string field_name2 = common_fd[k++];    
	vector<string>::iterator res1=find(field_names1.begin(),field_names1.end(),field_name1); 
	vector<string>::iterator res2=find(field_names2.begin(),field_names2.end(),field_name2); 
	
	while(res1 == field_names1.end( )||res2 == field_names2.end( ))
	{
		field_name1 = common_fd[k++];
		field_name2 = common_fd[k++];   //sorted by this field
		res1=find(field_names1.begin(),field_names1.end(),field_name1); 
		res2=find(field_names2.begin(),field_names2.end(),field_name2); 
	}
	if(rp1->getNumOfBlocks()<10||rp2->getNumOfBlocks()<10) 
	{
	    cerr<<"This Join can be in one pass! "<<endl;
	    return OnePass_Join(relation_name1,relation_name2,common_fd);
	}
    int tuplesnum_perblock1 =sch1.getTuplesPerBlock();
	int tuplesnum_perblock2 =sch2.getTuplesPerBlock();
	vector<string> sublist1=Get_sublists(relation_name1,field_name1);
	vector<string> sublist2=Get_sublists(relation_name2,field_name2);
	Relation* sublist_p1,* sublist_p2;
	Block* bp1, *bp2;

	int *blocks1 = new int[sublist1.size()];  //record the number of blocks of each sublist having been written to memory
	int sum1=rp1->getNumOfTuples();
	int *blocks2 = new int[sublist2.size()];             
	int sum2=rp2->getNumOfTuples();
    int i=0;
	for(i=0;i<sublist1.size();i++)  //put first block of each sublist of relation 1 to memory
	{
		sublist_p1 = schema_manager.getRelation(sublist1[i]);
		sublist_p1->getBlock(0,i);
		blocks1[i]=0;
	}
	for(int j=0;i<sublist1.size()+sublist2.size();i++,j++) //put first block of each sublist of relation 2 to memory
	{
		sublist_p2 = schema_manager.getRelation(sublist2[j]);
		sublist_p2->getBlock(0,i);
		blocks2[j]=0;
	}
	while(sum1>0&&sum2>0)
	{
		TupleAddr minsub1=Get_Min(field_name1,0,sublist1.size());
		TupleAddr minsub2=Get_Min(field_name2,sublist1.size(),sublist2.size());
		bp1=mem.getBlock(minsub1.block_index);
		bp2=mem.getBlock(minsub2.block_index);
		Tuple minsub1_t=bp1->getTuple(minsub1.offset);
		Tuple minsub2_t=bp2->getTuple(minsub2.offset);
		
		if(Join_Tuple(relation_name1,relation_name2,minsub1_t,minsub2_t,common_fd).isNull())   //can't join
		{    
			if(!Larger_Field(minsub1_t,minsub2_t,field_name1,field_name2)) 
			{
			    bp1->nullTuple(minsub1.offset);
				sum1--;
				if(bp1->getNumTuples()==0)       
				{
					sublist_p1 = schema_manager.getRelation(sublist1[minsub1.block_index]);
					if(blocks1[minsub1.block_index]+1<sublist_p1->getNumOfBlocks())
					{
						blocks1[minsub1.block_index]=blocks1[minsub1.block_index]+1;
						sublist_p1->getBlock(blocks1[minsub1.block_index],minsub1.block_index);
					}
				}
			}
			else 
			{
			    bp2->nullTuple(minsub2.offset);
				sum2--;
				if(bp2->getNumTuples()==0)     //if this block of sublist is empty, put next block of this sublist
				{
					int sub2_index=minsub2.block_index-sublist1.size();
					sublist_p2 = schema_manager.getRelation(sublist2[sub2_index]);
					if(blocks2[sub2_index]+1<sublist_p2->getNumOfBlocks())
					{
						blocks2[sub2_index]=blocks2[sub2_index]+1;
						sublist_p2->getBlock(blocks2[sub2_index],minsub2.block_index);
					}
				}
			}
		}
		else  //can join
		{
			vector<Tuple> mintuples1,mintuples2;
			for(i=0;i<sublist1.size();i++)
			{
				bp1=mem.getBlock(i);
				for(int j=0;j<tuplesnum_perblock1;j++)
				{
					if(bp1->getTuple(j).isNull())continue;
					
					if(!Join_Tuple(relation_name1,relation_name2,bp1->getTuple(j),minsub2_t,common_fd).isNull())
					{
						mintuples1.push_back(bp1->getTuple(j));
						bp1->nullTuple(j);
						sum1--;
						if(bp1->getNumTuples()==0)      //if this block of sublist is empty, put next block of this sublist
						{
							sublist_p1 = schema_manager.getRelation(sublist1[i]);
							if(blocks1[i]+1<sublist_p1->getNumOfBlocks())
							{
								blocks1[i]=blocks1[i]+1;
								sublist_p1->getBlock(blocks1[i],i);
							}
							j=-1;  
						}
					}
				}
			}
			for(int k=0;i<sublist1.size()+sublist2.size();i++,k++)
			{
				bp2=mem.getBlock(i);
				for(int j=0;j<tuplesnum_perblock2;j++)
				{
					if(bp2->getTuple(j).isNull())continue;
					if(!Join_Tuple(relation_name1,relation_name2,minsub1_t,bp2->getTuple(j),common_fd).isNull())
					{
						mintuples2.push_back(bp2->getTuple(j));
						bp2->nullTuple(j);
						sum2--;
						if(bp2->getNumTuples()==0)       
						{
							sublist_p2 = schema_manager.getRelation(sublist2[k]);
							if(blocks2[k]+1<sublist_p2->getNumOfBlocks())
							{
								blocks2[k]=blocks2[k]+1;
								sublist_p2->getBlock(blocks2[k],i);
							}
							j=-1;
						}
					}
				}
			}
			while(!mintuples1.empty())
			{
				for(int r=0;r<mintuples2.size();r++)
				    res.push_back(Join_Tuple(relation_name1,relation_name2,mintuples1.back(),mintuples2[r],common_fd));
				mintuples1.pop_back();
			}
		}
	}
	return res;
}
//return the joined result if two tuples can do the natural join
Tuple Operations::Join_Tuple(Tuple t1,Tuple t2)
{
	vector<enum FIELD_TYPE> field_types;
	vector<string> field_names,common_fd;  
	Schema sch1 =  t1.getSchema();
	Schema sch2 =  t2.getSchema();
	for(int i=0;i<sch1.getFieldNames().size();i++)
	{
		if(sch2.fieldNameExists(sch1.getFieldName(i)))
		{
			common_fd.push_back(sch1.getFieldName(i));
			continue;
		}
		else
		{
			field_names.push_back(sch1.getFieldName(i));
			field_types.push_back(sch1.getFieldType(i));
		}
	}
	for(int i=0;i<sch2.getFieldNames().size();i++)
	{
		field_names.push_back(sch2.getFieldName(i));
		field_types.push_back(sch2.getFieldType(i));
	}
	string relation_name="Joined_Relation:";
	for(int i=0;i<common_fd.size();i++) 
	   relation_name+=common_fd[i];
	Relation* jp;
	Schema sch(field_names,field_types);
	while(schema_manager.relationExists(relation_name))
	{
		if(schema_manager.getRelation(relation_name)->getSchema()==sch)
		{
			jp=schema_manager.getRelation(relation_name);
			break;
		}
		relation_name+="a";
	}
	if(!schema_manager.relationExists(relation_name)) 
	   jp=Create_Table(relation_name,field_names,field_types);
	   
	Tuple t = jp->createTuple();
	if(t1.isNull()||t2.isNull())
	{
		t.null();
		return t;
	}
	for(int i=0;i<sch1.getFieldNames().size();i++)
	{
		string fdname=sch1.getFieldName(i);
		if(sch2.fieldNameExists(fdname))
		{
			if(sch1.getFieldType(fdname)==INT&&sch2.getFieldType(fdname)==INT)
			{
				if(t1.getField(fdname).integer==t2.getField(fdname).integer)
				{
					t.setField(fdname,t1.getField(fdname).integer);
					continue;
				}
				else
				{
				    t.null();
				    return t;
				}
			}
			else if(sch1.getFieldType(fdname)==STR20&&sch2.getFieldType(fdname)==STR20)
			{
				if(*t1.getField(fdname).str==*t2.getField(fdname).str)
				{
					t.setField(fdname,*t1.getField(fdname).str);
					continue;
				}
				else
				{
				    t.null();
				    return t;
				}
			}
			else 
			{
				t.null();
				return t;
			}
		}
		if(sch1.getFieldType(fdname)==INT) 
		   t.setField(fdname,t1.getField(fdname).integer);
		else  
		   t.setField(fdname,*t1.getField(fdname).str);
	}
	for(int i=0;i<sch2.getFieldNames().size();i++)
	{
		string fdname=sch2.getFieldName(i);
		if(sch1.fieldNameExists(fdname))
		{
			continue;
		}
		if(sch2.getFieldType(fdname)==INT) 
		   t.setField(fdname,t2.getField(fdname).integer);
		else  t.setField(fdname,*t2.getField(fdname).str);
	}
	if(!t.isNull()) 
	   cerr<<t<<endl;
	return t;
}

Tuple Operations::Join_Tuple(string relation_name1,string relation_name2,Tuple t1,Tuple t2,vector<string> common_fd)
{
	vector<enum FIELD_TYPE> field_types;
	vector<string> field_names; 
	Schema sch1 =  t1.getSchema();
	Schema sch2 =  t2.getSchema();
	for(int i=0;i<sch1.getFieldNames().size();i++)
	{
		field_names.push_back(sch1.getFieldName(i));
		field_types.push_back(sch1.getFieldType(i));
	}
	for(int i=0;i<sch2.getFieldNames().size();i++)
	{
		field_names.push_back(sch2.getFieldName(i));
		field_types.push_back(sch2.getFieldType(i));
	}
	string relation_name="Joined_Relation:";
	for(int i=0;i<common_fd.size();i++) relation_name+=common_fd[i];
	Relation* jp;
	Schema sch(field_names,field_types);
	while(schema_manager.relationExists(relation_name))
	{
		if(schema_manager.getRelation(relation_name)->getSchema()==sch)
		{
			jp=schema_manager.getRelation(relation_name);
			break;
		}
		relation_name+="a";
	}
	if(!schema_manager.relationExists(relation_name)) jp=Create_Table(relation_name,field_names,field_types);

	Tuple t = jp->createTuple();
	if(t1.isNull()||t2.isNull())
	{
		t.null();
		return t;
	}
	for(int i=0;i<sch2.getFieldNames().size();i++)
	{
		string fdname2=sch2.getFieldName(i);
		if(sch2.getFieldType(fdname2)==INT) 
		   t.setField(fdname2,t2.getField(fdname2).integer);
		else  
		   t.setField(fdname2,*t2.getField(fdname2).str);
	}

	for(int i=0;i<sch1.getFieldNames().size();i++)
	{
		string fdname1=sch1.getFieldName(i);
		string Table_Name1=fdname1.substr(0,fdname1.find_last_of("."));
		string subfdname1=fdname1.substr(fdname1.find_last_of(".")+1);

		if(sch1.getFieldType(fdname1)==INT) 
		   t.setField(fdname1,t1.getField(fdname1).integer);
		else  
		   t.setField(fdname1,*t1.getField(fdname1).str);

		for(int j=0;j<sch2.getFieldNames().size();j++)
		{
			string fdname2=sch2.getFieldName(j);
			string Table_Name2=fdname2.substr(0,fdname2.find_last_of("."));
			string subfdname2=fdname2.substr(fdname2.find_last_of(".")+1);
			vector<string>::iterator res1=find(common_fd.begin(),common_fd.end(),fdname1);
			vector<string>::iterator res2=find(common_fd.begin(),common_fd.end(),fdname2);
	
			if(subfdname1==subfdname2)
			{
				if((res1 != common_fd.end( ))&&(res2 != common_fd.end( )))
				{
				if(sch1.getFieldType(fdname1)==INT&&sch2.getFieldType(fdname2)==INT)
				{
					if(t1.getField(fdname1).integer==t2.getField(fdname2).integer)
					{
						t.setField(fdname1,t1.getField(fdname1).integer);
						t.setField(fdname2,t1.getField(fdname1).integer);
						continue;
					}
					else
					{
					    t.null();
					    return t;
					}
				}
				else if(sch1.getFieldType(fdname1)==STR20&&sch2.getFieldType(fdname2)==STR20)
				{
					if(*t1.getField(fdname1).str==*t2.getField(fdname2).str)
					{
						t.setField(fdname1,*t1.getField(fdname1).str);
						t.setField(fdname2,*t1.getField(fdname1).str);
						continue;
					}
					else
					{
					    t.null();
					    return t;
					}
				}
				else 
				{
					t.null();
					return t;
				}
				}
			}
			else continue;
		}
	}
	return t;
}

bool Operations::Larger_Field(Tuple t1,Tuple t2,string field_name1,string field_name2)
{
	if(t1.isNull()||t2.isNull())
	   return false;
	Schema sch1 =  t1.getSchema();
	Schema sch2 =  t2.getSchema();
	if(!sch1.fieldNameExists(field_name1)||!sch2.fieldNameExists(field_name2))
	{
		cerr<<"field name does not exist. "<<endl;
		return false;
	}
	if(sch1.getFieldType(field_name1)==INT && sch2.getFieldType(field_name2)==INT)
	{
		if(t1.getField(field_name1).integer>t2.getField(field_name2).integer) 
			return true; 
		else 
			return false; 
	}
	else if(sch1.getFieldType(field_name1)==STR20&& sch2.getFieldType(field_name2)==STR20)
	{
		if(*t1.getField(field_name1).str>*t2.getField(field_name2).str) 
			return true; 
		else 
			return false; 
	}
	else 
	    return false;	
}