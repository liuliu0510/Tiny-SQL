#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <string>
#include <stack>
#include <queue>
#include <vector>
#include <set>
#include "Tuple.h"
#include "Field.h"
#include "Relation.h"

using namespace std;
void err_out_START(const char str[]);
void scan(int statement);
	

#define GREEN_TEXT  "\e[1;32m"
#define RED_TEXT "\e[1;31m"
#define NORMAL_TEXT "\e[0m"
#define HLINE  "---------------"

enum QueryTree_TYPE {
    PROJECTION,   //projection operation
    SELECTION, //select operation
    SORT, //sorting operation
    INSERT,  //Insert operation: insert selected tuples to a table
    CROSS_PRODUCT ,//cross-join operation, can be optimized to natural join operation, for some cross-join operation and select operation
    TABLE,//table scan operation: get all tuples from a specified table
    DUP_ELIMINATE//duplicate elimination operation
};

enum QueryExp_TYPE{
    QueryExp_ILLEGAL ,
    COLUMN  ,//leaf tree node with a column name
    LITERAL, //leaf tree node with a string
    INTEGER, //leaf tree node with an integer
    OPERATER ,//an operator tree node
    LEFT      //special node for left parenthesis: used with the operator stack 
};

/* precedence  */
#define TIMES_DIVIDES 5//* /
#define PLUS_MINUS    4//+ -
#define COMPARE       3//comparing operators including >,<,and =
#define NOT_PCD       2//NOT
#define AND_PCD       1//AND
#define OR_PCD        0//OR
                               
class QueryTree {
	public:
	int type ; 
	vector<string> info;
	QueryTree *left, *right, *parent;
	QueryTree(enum QueryTree_TYPE type, QueryTree *parent); 
	void print( int );
	void free() ;
	vector<Tuple> exec(bool print, string *table_name);
}; 
class QueryExp{
	public:
	enum QueryExp_TYPE type ; 
	int number;
	string str;
	set<string> tables ;
    QueryExp *left, *right;
	QueryExp() ;
	QueryExp(enum QueryExp_TYPE , int);
	QueryExp(enum QueryExp_TYPE , string);
	QueryExp(enum QueryExp_TYPE , int, string);
	QueryExp* Optimize_Select(map<string, QueryExp *>* select_operation) ;
	QueryExp* Optimize_Join(vector<string> &commons, map<string, bool> &joined_key) ;
	bool judge(Tuple t);
	void print(int level) ;
	void free() ;
	enum FIELD_TYPE field_type(Tuple ) ;
	private:
	union Field judge_(Tuple t) ;
};

#endif
