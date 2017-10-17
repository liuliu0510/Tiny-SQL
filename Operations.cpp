#include "Operations.h"

Operations * Operations::op=NULL;

 
bool Operations::fieldEqual(Tuple t1,Tuple t2,string field_name)
{
	if(t1.isNull()||t2.isNull())   
	   return false;
	Schema sch1 =  t1.getSchema();
	Schema sch2 =  t2.getSchema();
	if(!sch1.fieldNameExists(field_name)||!sch2.fieldNameExists(field_name))
	{
		cerr<<"field name does not exist. "<<endl;
		return false;
	}
	if(sch1.getFieldType(field_name)==INT && sch2.getFieldType(field_name)==INT)
	{
		if(t1.getField(field_name).integer==t2.getField(field_name).integer) 
		   return true; 
	}

	else if(sch1.getFieldType(field_name)==STR20&& sch2.getFieldType(field_name)==STR20)
	{
		if(*t1.getField(field_name).str==*t2.getField(field_name).str) 
		   return true; 
	}
	else 
	     return false;	
}


int max(int a,int b)
{
	return a>b?a:b;
}
int min(int a,int b)
{
	return a<b?a:b;
}






