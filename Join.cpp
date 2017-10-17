#include "Operations.h"

//Cross Join
vector<Tuple> Operations::Product(string relation_name1,string relation_name2)
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
	vector<string> fd_names;        // for new schema
	vector<enum FIELD_TYPE> fd_types;
	Block* bp0,*bp;
	bp0=mem.getBlock(0); 
	bp0->clear();
	for(int i=0;i<sch1.getFieldNames().size();i++)
	{
		fd_names.push_back(sch1.getFieldName(i));
		fd_types.push_back(sch1.getFieldType(i));
	}
	for(int i=0;i<sch2.getFieldNames().size();i++)
	{
		fd_names.push_back(sch2.getFieldName(i));
		fd_types.push_back(sch2.getFieldType(i));
	}
	string relation_name=relation_name1+"&"+relation_name2;
	if(schema_manager.relationExists(relation_name))
	   Drop_Table(relation_name);
	Relation* ptr=Create_Table(relation_name,fd_names,fd_types);
	if(ptr==NULL){
		cerr<<"Create Table Failed!";
		return res;
	}
	Relation* Rsmall=(rp1->getNumOfBlocks()<rp2->getNumOfBlocks())?rp1:rp2;
	Relation* Rlarge=(rp1->getNumOfBlocks()<rp2->getNumOfBlocks())?rp2:rp1;

	if(!Rsmall->getBlocks(0,1,Rsmall->getNumOfBlocks()))
	{
		cerr << "Can't be done in ONE PASS! "<<endl;
		return res;
	}
	Tuple tuple = ptr->createTuple();
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
					int u,w;
					if(Rsmall==rp1){
						for(u=0;u<Tsmall.getNumOfFields();u++)
						{
							if(sch1.getFieldType(u)==INT)
								tuple.setField(u,Tsmall.getField(u).integer);
							else tuple.setField(u,*Tsmall.getField(u).str);
						}
						for(w=0;w<Tlarge.getNumOfFields();w++,u++)
						{
							if(sch2.getFieldType(w)==INT)
								tuple.setField(u,Tlarge.getField(w).integer);
							else tuple.setField(u,*Tlarge.getField(w).str);
						}
					}
					else{
						for(u=0;u<Tlarge.getNumOfFields();u++)
						{
							if(sch1.getFieldType(u)==INT)
								tuple.setField(u,Tlarge.getField(u).integer);
							else tuple.setField(u,*Tlarge.getField(u).str);
						}
						for(w=0;w<Tsmall.getNumOfFields();w++,u++)
						{
							if(sch2.getFieldType(w)==INT)
								tuple.setField(u,Tsmall.getField(w).integer);
							else tuple.setField(u,*Tsmall.getField(w).str);
						}
					}   
					res.push_back(tuple);	
				}
			}
		}
	}
	return res;
}


vector<Tuple> Operations::Join_Tree(JoinNode &root)
{
	vector<Tuple> res;
	if(root.left!=NULL && root.right!=NULL) 
	{
		Relation * L_relation;
		Relation * R_relation;
		if(root.left->left==NULL)
			L_relation=root.left->m.relation_ptr;
		else 
		{
			Drop_Table(root.m.relation_name+"left");
			L_relation=Create_Table(root.m.relation_name+"left",Join_Tree(*root.left));
		}
		if(root.right->right==NULL)
			R_relation=root.right->m.relation_ptr;
		else
		{ 
		    Drop_Table(root.m.relation_name+"right");
		    R_relation=Create_Table(root.m.relation_name+"right",Join_Tree(*root.right));
		}
		    res=TwoPass_Join(L_relation->getRelationName(),R_relation->getRelationName());
		    return res;
	}
	else
		return res;
}
vector<Tuple> Operations::Join_Tree(JoinNode & root,vector<string> common_fd)
{
	vector<Tuple> res;
	if(root.left!=NULL && root.right!=NULL) 
	{
		Relation * L_relation;
		Relation * R_relation;
		if(root.left->left==NULL)
			L_relation=root.left->m.relation_ptr;
		else 
		{
			JoinNode *leftLeft=root.left->left;
			JoinNode *leftRight=root.left->right;
			string Lrelation_name;
			if(leftLeft->m.relation_name<leftRight->m.relation_name)
			    Lrelation_name = leftLeft->m.relation_name+"-x-"+leftRight->m.relation_name;
			else 
			    Lrelation_name = leftRight->m.relation_name+"-x-"+leftLeft->m.relation_name;
			Drop_Table(Lrelation_name);
			L_relation=Create_Table(Lrelation_name,Join_Tree(*root.left, common_fd));
		}
		if(root.right->right==NULL)
			R_relation=root.right->m.relation_ptr;
		else
		{ 
			JoinNode *rightLeft=root.right->left;
			JoinNode *rightRight=root.right->right;
			string Rrelation_name;
			if(rightLeft->m.relation_name<rightRight->m.relation_name)
			   Rrelation_name = rightLeft->m.relation_name+"-x-"+rightRight->m.relation_name;
			else 
			   Rrelation_name = rightRight->m.relation_name+"-x-"+rightLeft->m.relation_name;
			Drop_Table(Rrelation_name);
			R_relation=Create_Table(Rrelation_name,Join_Tree(*root.right, common_fd));
		}
		string Lrelation_name,Rrelation_name;
		Lrelation_name=L_relation->getRelationName();
		Rrelation_name=R_relation->getRelationName();

		if(Lrelation_name<Rrelation_name){
			res=TwoPass_Join(L_relation->getRelationName(),R_relation->getRelationName(),common_fd);}
		else{
			res=TwoPass_Join(R_relation->getRelationName(),L_relation->getRelationName(),common_fd);}
		cerr<<root.m.relation_name<<" has tuples "<< res.size()<<endl;
		return res;
	}
	else
		return res;
}

vector<Tuple> Operations::Join_Table(vector<string> relation_names)
{
	relation_data* Rdata=new relation_data[relation_names.size()];
	vector<Tuple> Res;
	vector<int> v;
	int relation_num=relation_names.size();
	for(int i=0;i<relation_num;i++)
	{
		Rdata[i]=Update_Relation(relation_names[i]);
		v.push_back(i);
	}
	map<string,int> size,cost;
	map<string,JoinNode> Rcost;
	for(int i=0;i<relation_num;i++)
	{
		size[relation_names[i]]=0;
		cost[relation_names[i]]=0;
		Rcost[relation_names[i]]=JoinNode(Update_Relation(relation_names[i]));
	}
	vector<vector<int> > merg=Get_Elements(relation_num, 2,v);
	for(int i=0;i<merg.size();i++)
	{
		string relation_name;
		for(int j=0;j<merg[i].size();j++)
		{
			if(j>0) relation_name +="*";
			relation_name+=relation_names[merg[i][j]];
			
		}
		cost[relation_name]=0;

		Rcost[relation_name]=JoinNode(Get_Joined_data(Update_Relation(relation_names[merg[i][0]]),Update_Relation(relation_names[merg[i][1]])),&(Rcost[relation_names[merg[i][0]]]),&(Rcost[relation_names[merg[i][1]]]));
		size[relation_name]=Rcost[relation_name].m.numT;
	}
	
	for(int k=3;k<=relation_names.size();k++)
	{
		vector<vector<int> > merg=Get_Elements(relation_names.size(),k,v);

		for(int i=0;i<merg.size();i++)
		{
			string relation_name;
			for(int j=0;j<merg[i].size();j++)
			{
				if(j>0) relation_name +="*";
				relation_name+=relation_names[merg[i][j]];
			}
			cerr<<relation_name<<" : "<<endl;
			cost[relation_name]=INT_MAX;
			string lp1,lp2;
			
			for(int j=1;j<=merg[i].size()/2;j++)   //partition
			{
				vector<vector<int> > part1=Get_Elements(k,j,merg[i]);
				for(int l=0;l<part1.size();l++)  //for each partition
				{
					string relation_name1,relation_name2;
					for(int m=0;m<merg[i].size();m++)
					{
						for(int n=0;n<part1[l].size();n++)
						{
							if(merg[i][m]==part1[l][n]) 
							{
								if(relation_name1.size()!=0)relation_name1+='*'; 
								relation_name1+=relation_names[merg[i][m]];
								break;
							}
							if(n==part1[l].size()-1) 
							{
								if(relation_name2.size()!=0)relation_name2+='*'; 
								relation_name2+=relation_names[merg[i][m]];
							}		
						}
					}
					cerr<<relation_name1<<"   "<<relation_name2<<endl;
					int costThis=cost[relation_name1]+cost[relation_name2]+size[relation_name1]+size[relation_name2];
					if(cost[relation_name]>costThis) 
					{
						cost[relation_name]=costThis;
						lp1=relation_name1;lp2=relation_name2;
					}
				}
			}
			Rcost[lp1].m.print();
			Rcost[lp2].m.print();
			Rcost[relation_name]=JoinNode(Get_Joined_data(Rcost[lp1].m,Rcost[lp2].m),&Rcost[lp1],&Rcost[lp2]);
			size[relation_name]=Rcost[relation_name].m.numT;
		}
	}

	string relation_name;
	for(int i=0;i<relation_num;i++)
	{
		if(relation_name.size()!=0)relation_name+='*'; 
		relation_name+=relation_names[i];
	}
	JoinNode Join=Rcost[relation_name];
	JoinNode *tmp=&Join;
	tmp->print();
	Res=Join_Tree(Join);
	return Res;
}

vector<Tuple> Operations::Join_Table(vector<string> relation_names,vector<string> common_fd)
{
	relation_data* Rdata=new relation_data[relation_names.size()];
	vector<Tuple> Res;
	vector<int> v;
	int relation_num=relation_names.size();
	for(int i=0;i<relation_num;i++)
	{
		Rdata[i]=Update_Relation(relation_names[i]);
		v.push_back(i);
	}
	map<string,int> size,cost;
	map<string,JoinNode> Rcost;
	for(int i=0;i<relation_num;i++)
	{
		size[relation_names[i]]=0;cost[relation_names[i]]=0;
		Rcost[relation_names[i]]=JoinNode(Update_Relation(relation_names[i]));
	}
	vector<vector<int> > merg=Get_Elements(relation_num, 2,v);
	for(int i=0;i<merg.size();i++)
	{
		string relation_name;
		for(int j=0;j<merg[i].size();j++)
		{
			if(j>0) relation_name +="*";
			relation_name+=relation_names[merg[i][j]];
		}
		cost[relation_name]=0;
		Rcost[relation_name]=JoinNode(Get_Joined_data(Update_Relation(relation_names[merg[i][0]]),Update_Relation(relation_names[merg[i][1]])),&(Rcost[relation_names[merg[i][0]]]),&(Rcost[relation_names[merg[i][1]]]));
		size[relation_name]=Rcost[relation_name].m.numT;
	}
	
	for(int k=3;k<=relation_names.size();k++)
	{
		vector<vector<int> > merg=Get_Elements(relation_names.size(),k,v);

		for(int i=0;i<merg.size();i++)
		{
			string relation_name;
			for(int j=0;j<merg[i].size();j++)
			{
				if(j>0) relation_name +="*";
				relation_name+=relation_names[merg[i][j]];
			}
			cost[relation_name]=INT_MAX;
			string lp1,lp2;
			for(int j=1;j<=merg[i].size()/2;j++)   //partition
			{
				vector<vector<int> > part1=Get_Elements(k,j,merg[i]);
				for(int l=0;l<part1.size();l++)  //for each partition
				{
					string relation_name1,relation_name2;
					for(int m=0;m<merg[i].size();m++)
					{
						for(int n=0;n<part1[l].size();n++)
						{
							if(merg[i][m]==part1[l][n]) 
							{
								if(relation_name1.size()!=0)relation_name1+='*'; 
								relation_name1+=relation_names[merg[i][m]];
								break;
							}
							if(n==part1[l].size()-1) 
							{
								if(relation_name2.size()!=0)relation_name2+='*'; 
								relation_name2+=relation_names[merg[i][m]];
							}		
						}
					}
					int costThis=cost[relation_name1]+cost[relation_name2]+size[relation_name1]+size[relation_name2];
					if(cost[relation_name]>costThis) 
					{
						cost[relation_name]=costThis;
						lp1=relation_name1;lp2=relation_name2;
					}
				}
			}
			Rcost[lp1].m.print();
			Rcost[lp2].m.print();
			Rcost[relation_name]=JoinNode(Get_Joined_data(Rcost[lp1].m,Rcost[lp2].m),&Rcost[lp1],&Rcost[lp2]);
			size[relation_name]=Rcost[relation_name].m.numT;
		}
	}
	string relation_name;
	for(int i=0;i<relation_num;i++)
	{
		if(relation_name.size()!=0)relation_name+='*'; 
		relation_name+=relation_names[i];
	}
	JoinNode Join=Rcost[relation_name];
	JoinNode *tmp=&Join;
	tmp->print();
	Res=Join_Tree(Join,common_fd);
	return Res;

}

relation_data Operations::Get_Joined_data(relation_data Rdata1, relation_data Rdata2)
{
	//relation_data result;
	int totaltuples=Rdata1.numT*Rdata2.numT;
	Schema  sch1=Rdata1.schema;
	Schema  sch2=Rdata2.schema;

	vector<string> fd_names1=sch1.getFieldNames();
	vector<string> fd_names2=sch2.getFieldNames(); 
	vector<string> fd_names;
	vector<enum FIELD_TYPE> fd_types;
	vector<int>  offset1,offset2;
	vector<int>  Vec;
	for(int i =0;i<sch1.getNumOfFields();i++)
	{
		if(sch2.fieldNameExists(sch1.getFieldName(i)))
		{
			offset1.push_back(i);
			offset2.push_back(sch2.getFieldOffset(sch1.getFieldName(i)));
		}
		else
		{
			fd_names.push_back(sch1.getFieldName(i));
			fd_types.push_back(sch1.getFieldType(i));
			Vec.push_back(Rdata1.Vec[i]);
		}
	}
	for(int i =0;i<sch2.getNumOfFields();i++)
	{
		fd_types.push_back(sch2.getFieldType(i));
		fd_names.push_back(sch2.getFieldName(i));
		if(sch1.fieldNameExists(sch2.getFieldName(i)))
		{
			Vec.push_back(min(Rdata1.Vec[sch1.getFieldOffset(sch2.getFieldName(i))],Rdata2.Vec[i]));
		}
		else Vec.push_back(Rdata2.Vec[i]);
	}
	while(!offset1.empty())
	{
		if(Rdata1.Vec[offset1.back()]==0||Rdata2.Vec[offset2.back()]==0) 
		   totaltuples=0;
		else 
		   totaltuples/=max(Rdata1.Vec[offset1.back()],Rdata2.Vec[offset2.back()]);
		offset1.pop_back();
		offset2.pop_back();
	}
	string Rname=Rdata1.relation_name+"*"+Rdata2.relation_name;
	Schema sch(fd_names,fd_types);
	return relation_data(Vec,totaltuples,Rname,sch);
}

relation_data Operations::Update_Relation(string relation_name)
{
	Relation* rp = schema_manager.getRelation(relation_name);
	int tpnum=rp->getNumOfTuples();
	Schema s=rp->getSchema();
	int *v=new int[s.getNumOfFields()];
	int Int_fd=0,Str_fd=0;
	for(int i=0;i<s.getNumOfFields();i++) 
	{
		v[i]=0;
		if(s.getFieldType(i)==INT) Int_fd++;
		else Str_fd++;
	}
	vector<Tuple> rt;
	vector<int> * Vec_Int = new vector<int>[Int_fd];
	vector<string>* Vec_Str=new vector<string>[Str_fd];
    int tuplesnum_perblock=s.getTuplesPerBlock();
	for(int i=0;i<rp->getNumOfBlocks();i++)
	{
		rp->getBlock(i,0);
		Block *bp=mem.getBlock(0);
		for(int j=0;j<tuplesnum_perblock;j++)
		{
			Tuple t=bp->getTuple(j);
			if(t.isNull()) continue;
			else rt.push_back(t);
		}
	}
	while(!rt.empty())
	{
		Tuple t= rt.back();
		Int_fd=0;
		Str_fd=0;
		for(int j=0;j<t.getNumOfFields();j++)
		{
			if(s.getFieldType(j)==INT)
			{
				if(Vec_Int[Int_fd].empty())
				{
					Vec_Int[Int_fd].push_back(t.getField(j).integer);
					Int_fd++;
					continue;
				}
				if(find(Vec_Int[Int_fd].begin(),Vec_Int[Int_fd].end(),t.getField(j).integer)==Vec_Int[Int_fd].end())
				{
					Vec_Int[Int_fd].push_back(t.getField(j).integer);
				}
				Int_fd++;
			}
			if(s.getFieldType(j)==STR20)
			{
				if(Vec_Str[Str_fd].empty())
				{
					Vec_Str[Str_fd].push_back(*t.getField(j).str);
					Str_fd++;
					continue;
				}
				if(find(Vec_Str[Str_fd].begin(),Vec_Str[Str_fd].end(),*t.getField(j).str)==Vec_Str[Str_fd].end())
				{
					Vec_Str[Str_fd].push_back(*t.getField(j).str);
				}
				Str_fd++;
			}
		}
		rt.pop_back();
	}
	Int_fd=0;
	Str_fd=0;
	for(int i=0;i<s.getNumOfFields();i++)
	{
		if(s.getFieldType(i)==INT)
			v[i]=Vec_Int[Int_fd++].size();
		else
			v[i]=Vec_Str[Str_fd++].size();
	}
	vector<int> Vec;
	for(int i=0;i<s.getNumOfFields();i++)
	{
		Vec.push_back(v[i]);
	}
	vector<string> fd_names=s.getFieldNames();
	vector<string> newfdnames;
	for(int i=0;i<fd_names.size();i++)
	{
		string oldname=fd_names[i];
		string newname=oldname.substr(oldname.find_last_of(".")+1);
		newfdnames.push_back(newname);
	}
	Schema new_schema(newfdnames,s.getFieldTypes());
	return relation_data(Vec,tpnum,relation_name,new_schema,rp);
}

void Operations::Pick_Elements(int a,int b,vector<int>v1,vector<int> &v2,vector<vector<int> > &merg)
{
	for(int i=v1.size()-a;i<=v1.size()-b;i++)
	{
		v2.push_back(v1[i]);
		if(b>1) 
		  Pick_Elements(v1.size()-1-i,b-1,v1,v2,merg);
		else
		{
			vector<int> x = v2;
			merg.push_back(x);
		}
		v2.pop_back();
	}
}
vector<vector<int> > Operations::Get_Elements(int a,int b,vector<int>v1)
{
	vector<vector<int> > merg;
	vector<int> v2;
	Pick_Elements(a, b,v1,v2,merg);
	return merg;
}
