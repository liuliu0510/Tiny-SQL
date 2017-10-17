#ifndef _OPERATIONS_H_                   // include file only once
#define _OPERATIONS_H_
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <string>
#include <iterator>
#include <vector>
#include <algorithm>
#include <cmath>
#include <map>
#include <list>
#include <climits>

#include "Block.h"
#include "Config.h"
#include "Disk.h"
#include "Field.h"
#include "MainMemory.h"
#include "Relation.h"
#include "Schema.h"
#include "SchemaManager.h"
#include "Tuple.h"
#include "DataStructures.h"
using namespace std;

class Operations{
private:
	MainMemory mem;
	Disk disk;
	clock_t Operations_start_time;
	static Operations *op;
    Operations():schema_manager(&mem, &disk) {  
		disk.resetDiskIOs();
		disk.resetDiskTimer();
		Operations_start_time = clock() ;
    }    //singleton

public:
	SchemaManager   schema_manager;

	int IO(){return disk.getDiskIOs();}
	void IOreset(){disk.resetDiskIOs();
		disk.resetDiskTimer();}
	static Operations *getInstance() {
        if (op == NULL) {
            op = new Operations();
			//cout<<"Physical operator initialized!"<<endl;
			//op->displayMem();
        }
        return op;
    }

    //create 
	Relation * Create_Table(string, vector<string> & , vector <enum FIELD_TYPE> &);
	Relation * Create_Table(string, vector<Tuple>);  
	//drop 
	void Drop_Table(string);
	//delete
    void Delete(string, condition);
    //insert 
	void Insert(string,	vector<string>,vector<string>,vector<int>);
	//select 
	vector<Tuple> Singletable_Select(string relation_name,condition con);
	//algorithms: one-pass and two-pass 						
	vector<Tuple> OnePass_Sort(string,string);  //one-pass algorithm
	vector<Tuple> TwoPass_Sort(string,string);  //two-pass algorithm
	vector<Tuple> OnePass_DupElimination(string);  //duplicate elimination ONE PASS algorithm(don't write back)
	vector<Tuple> TwoPass_DupElimination(string);  
    vector<Tuple> Sort_In_Memory(string,int,int);  //sort the tuples in memory and return the result 
    vector<string> Get_sublists(string,string); //return a list of sublists name in disk(each is a relation)
	TupleAddr Get_Min(string,int,int);  //return the location of the minimum tuple
	vector<TupleAddr*> Get_Dups(Tuple,int,int);//Find duplicates  
	bool Same_Tuple(Tuple,Tuple);   //judge if these two tuples are same

    bool Larger_Field(Tuple a,Tuple b,string field_name);  //judge if the field of a is larger than which of b
	bool Larger_Field(Tuple a,Tuple b,string field_name1,string field_name2);  //judge if the field of a is larger than another of b
	bool fieldEqual(Tuple a,Tuple b, string field_name);   //to see whether two tuples' fields are equal 

	//Join
	vector<Tuple> Product(string, string);   //cross join, one pass
	vector<Tuple> OnePass_Join(string, string);   //natural join, one pass
	vector<Tuple> OnePass_Join(string, string,vector<string>);    
	vector<Tuple> TwoPass_Join(string,string);  //natural join, two pass
	vector<Tuple> TwoPass_Join(string,string ,vector<string> );   			      
	Tuple Join_Tuple(Tuple t1,Tuple t2);   //return a joined tuple if can join, or an invalid tuple
	Tuple Join_Tuple(string ,string,Tuple,Tuple,vector<string>);

	 vector<Tuple> Join_Tree(JoinNode & );
	 vector<Tuple> Join_Tree(JoinNode & ,vector<string>);
	 vector<Tuple> Join_Table(vector<string>);
	 vector<Tuple> Join_Table(vector<string>,vector<string>);
	 relation_data Get_Joined_data(relation_data ,relation_data);  //compute the data after join
	 relation_data Update_Relation(string);
	 void Pick_Elements(int,int,vector<int>,vector<int>&,vector<vector<int> > &);   //pick some elements from vector
	 vector<vector<int> >Get_Elements(int ,int ,vector<int>);
};

void Append_Tuple(Relation*, MainMemory& ,int,Tuple&);	



#endif
