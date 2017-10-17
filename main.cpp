#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include "lex.yy.cpp"
#include "Operations.h"
#include "common.h"

extern Operations *p ;
extern stack<QueryExp*> output_s;
extern vector<string> tmp_relations ;
extern std::fstream output_stream;

void err_out_START(const char str[]) {
	if(YY_START == D_S_EXPECT_WHERE) {
		scan(DELETE_STATEMENT) ;
	}
	#ifndef  NO_ERROR_OUTPUT
	error_output << GREEN_TEXT << HLINE << endl << "Scan start:\t" <<
	str << NORMAL_TEXT << endl;
	#endif
}
void scan(int statement){
	char buffer[64];
	switch(statement)
	{
	case CREATE_TABLE_STATEMENT:
	strcpy(buffer, "CREATE TABLE") ;
	break;

	case DROP_TABLE_STATEMENT:
	strcpy(buffer, "DROP TABLE") ;
	break;
	
	case SELECT_STATEMENT:
	strcpy(buffer, "SELECT [DISTINCT]") ;
	break;

	case INSERT_STATEMENT:
	strcpy(buffer, "INSERT INTO") ;
	break;

	case DELETE_STATEMENT:
	strcpy(buffer, "DELETE FROM") ;
	break;

	default:
	perror("unknow statement");
	exit(EXIT_FAILURE);
	break;
	}
	
	#ifndef  NO_ERROR_OUTPUT
	error_output << RED_TEXT << "Scan:\t" <<
	buffer <<  endl << 
	HLINE << NORMAL_TEXT <<  endl  ;
	#endif
}
QueryExp* QueryExp::Optimize_Join(vector<string> &commons, map<string, bool> &joined_key ){
	if(this->type == OPERATER && this->str[0] == 'A'){
		if(this->tables.size() >= 2){
			QueryExp *Lexp = this->left->Optimize_Join(commons, joined_key) ;
			QueryExp *Rexp = this->right->Optimize_Join(commons , joined_key) ;
			if(Lexp!= NULL && Rexp != NULL)
			{
				this->left = Lexp;
				this->right = Rexp ;
				return this;
			}
			else if(Lexp== NULL && Rexp != NULL)
			{
				delete this;
				return Rexp ;
			}
			else if(Lexp!= NULL && Rexp == NULL)
			{
				delete this;
				return Lexp;
			}
			else
			{
				delete this;
				return NULL;
			}
		}
		else
			return this;
	}
	else if(this->type == OPERATER && this->str[0] == '=' && this->left->type == COLUMN && this->right->type == COLUMN )
	{
		if(this->tables.size() == 2)
		{
			string key, fd0, fd1;
			string table0, table1 ;
			unsigned long pos0 = this->left->str.rfind('.'), pos1 = this->right->str.rfind('.') ;
			set<string>::iterator iter = this->tables.begin();
			iter++;

			fd0 = string(this->left->str.substr(pos0 + 1) ) ;
			fd1 = string(this->right->str.substr( pos1 + 1) ) ;
			table0 = string(this->left->str.substr(0, pos0));
			table1 = string(this->right->str.substr(0, pos1));
			if(fd0 == fd1){
				if(table0 <= table1){
					key = string( table0 + "-x-"+ table1 ) ;
					if(joined_key[key] == false){
						commons.push_back(this->left->str);
						commons.push_back(this->right->str);
						joined_key[key] = true;
					}
				}else{
					key = string( table1 + "-x-"+ table0 ) ;
					if(joined_key[key] == false){
						commons.push_back(this->right->str);
						commons.push_back(this->left->str);
						joined_key[key] = true;
					}
				}
				return NULL;
			}else{
				return this;
			}
		}else{
			return this;
		}
	}else{
		return this ;
	}
}
QueryExp* QueryExp::Optimize_Select(map<string, QueryExp *>* select_operation) {
	if(this->type == COLUMN){
		(*select_operation)[ * (this->tables.begin() ) ] = this ;
		return NULL ;
	}else if(this->type == OPERATER && this->str[0] == 'O'){
		if(this->tables.size() == 1){
			if(select_operation == NULL){
				return this ;
			}else{
				(*select_operation)[ * (this->tables.begin() ) ] = this ;
				return NULL;
			}
		}else if(this->tables.size() > 1){
			QueryExp *Lexp= this->left->Optimize_Select(NULL);
			QueryExp *Rexp = this->right->Optimize_Select(NULL);
			this->left = Lexp;
			this->right = Rexp ;
			return this;
		}else{
			Tuple *t = NULL;
			if(this->judge(*t)){
				QueryExp *ret = new QueryExp(INTEGER, 1);
				this->free() ;
				return ret;
			}else{
				QueryExp *ret = new QueryExp(INTEGER, 0);
				this->free() ;
				return ret;
			}
		}
	}else if(this->type == OPERATER && this->str[0] == 'A'){
		if(this->tables.size() == 1){
			if(select_operation == NULL){
				return this ;
			}else{
				(*select_operation)[* (this->tables.begin() ) ] = this ;
				return NULL;
			}
		}else if(this->tables.size() > 1){
			QueryExp *Lexp= this->left->Optimize_Select(select_operation);
			QueryExp *Rexp = this->right->Optimize_Select(select_operation);
			if(Lexp!= NULL && Rexp != NULL){
				this->left = Lexp;
				this->right = Rexp ;
				return this;
			}else if(Lexp== NULL && Rexp != NULL){
				delete this;
				return Rexp ;
			}else if(Lexp!= NULL && Rexp == NULL){
				delete this;
				return Lexp;
			}else{
				delete this;
				return NULL;
			}
		}else{
			Tuple *t = NULL;
			if(this->judge(*t)){
				QueryExp *ret = new QueryExp(INTEGER, 1);
				this->free() ;
				return ret;
			}else{
				QueryExp *ret = new QueryExp(INTEGER, 0);
				this->free() ;
				return ret;
			}
		}
	}else if( ( this->type == OPERATER && this->str[0] == '=' ) ||
	( this->type == OPERATER && this->str[0] == '>')||
	( this->type == OPERATER && this->str[0] == '<')||
	( this->type == OPERATER && this->str[0] == 'N' ) ){
		
		if(this->tables.size() == 1){
			if(select_operation == NULL){
				return this ;
			}else{
				(*select_operation)[* (this->tables.begin() ) ] = this ;
				return NULL;
			}
		}else if(this->tables.size() > 1){
			return this;
		}else{
			Tuple *t = NULL;
			if(this->judge(*t)){
				QueryExp *ret = new QueryExp(INTEGER, 1);
				this->free() ;
				return ret;
			}else{
				QueryExp *ret = new QueryExp(INTEGER, 0);
				this->free() ;
				return ret;
			}
		}
	}else{
		return this ;
	}
}
vector<Tuple> QueryTree::exec(bool print, string *table_name){
	vector<Tuple> ret ;
	if(this->type == INSERT){
		vector<Tuple> temp = this->left->exec( false, NULL ) ;
		if(temp.size() != 0){
			Schema sins_from  =  temp[0].getSchema() ;
			vector<enum FIELD_TYPE> field_types_from = sins_from.getFieldTypes() ;
			vector<string> field_names_from = sins_from.getFieldNames() ;
			if(field_types_from.size() == this->info.size() - 1){
				Schema sins_to = p->schema_manager.getSchema( this->info[0] ) ;
				vector<enum FIELD_TYPE> field_types_to ;
				vector<union Field> fields ;
				vector<string>::iterator iter0 = this->info.begin() ;
				vector<enum FIELD_TYPE>::iterator iter1 = field_types_from.begin();
				vector<string>::iterator iter2 = field_names_from.begin();
				vector<string> STRv;
				vector<int> INTv ;
				string table_n = (*iter0)  ;
				vector<string> field_names_to ;
				iter0 ++ ;
				for( ; iter0 != this->info.end()  ; iter0 ++, iter1++){
					unsigned long found = iter0->rfind('.')  ;
					string s_table ;
					if(found == std::string::npos){
						s_table = string( table_n + "." + (*iter0) ) ;
					}else{
						s_table = string( iter0->substr( iter0->rfind('.') + 1 )  ) ;
					}
					if( sins_to.fieldNameExists( *iter0 ) ){
						field_names_to.push_back(string(  *iter0)  ) ;
						if(sins_to.getFieldType( *iter0) == *iter1 ){
						}else{
							perror(  ": Type mismatch");
							return ret; 
						}
					}else{
						if(sins_to.fieldNameExists(s_table) ) {
							field_names_to.push_back(string(  s_table ) ) ;
							if(sins_to.getFieldType( s_table) == *iter1 ){
							}else{
								perror( ": Type mismatch");
								return ret; 
							}
						} else{
								perror( "exec: No such field");
						}
					}
				}	
				for(vector<Tuple>::iterator it_tuple = temp.begin(); it_tuple != temp.end(); it_tuple ++) {
					for(iter1 = field_types_from.begin(), iter2 = field_names_from.begin()  ; 
					iter1 != field_types_from.end()  ; iter1++, iter2++){
						if(*iter1 == INT){
							INTv.push_back( it_tuple->getField( *iter2).integer ) ;
						}else{
							STRv.push_back( *(it_tuple->getField( *iter2).str) ) ;
						}
					}
					p->Insert(table_n, field_names_to, STRv, INTv) ;
					INTv.clear();
					STRv.clear() ;
				}
			}else{
				perror("Size mismatch");
				return ret;
			}
		}else{
			return ret;
		}
	}else if(this->type == SORT){
		string table_n;
		if(this->left->type == TABLE && (output_s.empty() || output_s.top() == NULL) ){
			Schema s = p->schema_manager.getSchema( this->left->info[0] ) ;
			string s_table ;
			unsigned long found = this->info[0].rfind('.')  ;
			table_n = this->left->info[0] ;
			if(found == std::string::npos){
				s_table = string( table_n + "." + this->info[0]  ) ;
			}else{
				s_table = string( this->info[0].substr( this->info[0].rfind('.') + 1 )  ) ;
			}
			if( s.fieldNameExists( this->info[0] ) ){
				ret = p->TwoPass_Sort(table_n, this->info[0])  ;
			}else if(s.fieldNameExists(s_table) ) {
				ret = p->TwoPass_Sort(table_n, s_table)  ;
			}else{
				perror("No such field");
				return ret ;
			}
		}else{
			vector<Tuple> temp = this->left->exec( false, &table_n ) ;
			if(table_name != NULL) { (*table_name ) = string( this->info[0] ) ;}
			if(temp.size() != 0){
				Schema s  =  temp[0].getSchema() ;
				string temp_table_name = "temp_table" ;
				while(p->schema_manager.relationExists(temp_table_name) ){
					temp_table_name += "-a" ;
				}
				p->Create_Table(temp_table_name, temp ) ;
				 tmp_relations.push_back( temp_table_name ) ;
				unsigned long found = this->info[0].rfind('.')  ;
				string s_table ;
				if(found == std::string::npos){
					s_table = string( table_n + "." + this->info[0]  ) ;
				}else{
					s_table = string( this->info[0].substr( this->info[0].rfind('.') + 1 )  ) ;
				}
				if( s.fieldNameExists( this->info[0] ) ){
					ret = p->TwoPass_Sort(temp_table_name, this->info[0])  ;
				}else if(s.fieldNameExists(s_table) ) {
					ret = p->TwoPass_Sort(temp_table_name, s_table)  ;
				}else{
					perror("No such field");
					return ret ;
				}
			}else{
				return ret;
			}
		}
	}else if(this->type == DUP_ELIMINATE ){
		string table_n;
		if(this->left->type == TABLE){
			table_n = this->left->info[0] ;
			ret = p->TwoPass_DupElimination(table_n) ;
		}else{
			vector<Tuple> temp = this->left->exec( false , &table_n) ;
			if(table_name != NULL) { (*table_name ) = string( this->info[0] ) ;}
	
			if(temp.size() != 0){
				Schema s  =  temp[0].getSchema() ;
				string temp_table_name = "temp_table" ;
				while(p->schema_manager.relationExists(temp_table_name) ){
					temp_table_name += "-a" ;
				}
				p->Create_Table(temp_table_name, temp );
				 tmp_relations.push_back(temp_table_name  ) ;
				ret = p->TwoPass_DupElimination(temp_table_name) ;
			}else{
				return ret;
			}
		}
	}else if(this->type == PROJECTION ){
		string table_n;
		vector<Tuple> temp = this->left->exec( false, &table_n ) ;
		if(table_name != NULL) { (*table_name ) = string( this->info[0] ) ;}
		if(temp.size() != 0){
			Schema s  =  temp[0].getSchema() ;
			vector<string> field_names ;
			vector<enum FIELD_TYPE> field_types  ;
			for(vector<string>::iterator iter= this->info.begin(); iter != this->info.end(); iter++){
				unsigned long found = iter->rfind('.')  ;
				string s_table ;
				if(found == std::string::npos){
					s_table = string( table_n + "." + (*iter) ) ;
				}else{
					s_table = string( iter->substr( iter->rfind('.') + 1 )  ) ;
				}
				if( s.fieldNameExists( *iter ) ){
					field_names.push_back(string(*iter) ) ;
					field_types.push_back(s.getFieldType( *iter) ) ;
				}else{
					if(s.fieldNameExists(s_table) ) {
						field_names.push_back(string( s_table ) ) ;
						field_types.push_back( s.getFieldType( s_table )  );
					} else{
						perror( "exec: No such field");
					}
				}
			}
			string temp_table_name = "temp_table" ;
			Relation *rlt = NULL;
			while(p->schema_manager.relationExists(temp_table_name) ){
				temp_table_name += "-a" ;
			}
			rlt = p->Create_Table(temp_table_name, field_names, field_types) ;
			 tmp_relations.push_back(temp_table_name  ) ;
			for(vector<Tuple>::iterator tit = temp.begin(); tit != temp.end(); tit++){
				Tuple t = rlt->createTuple() ;
	
				for(vector<string>::iterator iter = field_names.begin(); iter != field_names.end() ; iter++){
					union Field f= tit->getField(*iter) ;
					if( s.getFieldType(*iter) == INT ){
						t.setField(  *iter,  f.integer ) ;
					}else{
						t.setField( *iter, *(f.str)) ;
					}
				}
				ret.push_back( t ) ;
			}
		}else{
			return ret;
		}
	}else if(this->type == CROSS_PRODUCT){
		vector<string> ptables;
		vector<Relation *> relations ;
		map<string, QueryExp *> select_operation ;
		vector<string> commons ;
		map<string, bool> joined_keys;

		vector<string>::iterator iter = ptables.begin();

		ptables.insert(ptables.end(), this->info.begin(), this->info.end() );
		
		if(output_s.empty() ){
		}else if(output_s.top()->type == INTEGER || output_s.top()->type == LITERAL ){
			Tuple *t = NULL;
			if(output_s.top()->judge(*t) ){
				/* WHERE clasuse always true */
		 		 while(! output_s.empty() ){ output_s.top()->free() ;output_s.pop();}
			}else{
				/* empty results */
				return ret; 
			}
		}else{
			QueryExp *optimized = output_s.top()->Optimize_Select(&select_operation) ;
			output_s.pop(); if(optimized != NULL){ output_s.push(optimized) ;}
			
			#ifdef DEBUG
			for(map<string, QueryExp *>::iterator iter = select_operation.begin(); iter != select_operation.end(); iter ++){
				cout << iter->first << "->" << endl;
				iter->second->print(0);
			}
			#endif

			if( ! output_s.empty() ){
				optimized = output_s.top()->Optimize_Join(commons, joined_keys) ;
				output_s.pop(); 
				if(optimized != NULL){ 
					output_s.push(optimized) ;
				}else{
					while(! output_s.empty() ){output_s.top()->free() ; output_s.pop();}
				}

				if(! output_s.empty()){
					#ifdef DEBUG
					output_s.top()->print(0); 
					#endif
				}
			}
			#ifdef DEBUG
			cerr << "commons: ";
			for(vector<string>::iterator iter = commons.begin(); iter != commons.end(); iter++){
				cerr<< *iter << " " ;
			}
			cerr << endl ;
			#endif
		}
		vector<string> to_drop ;
		for(vector<string>::iterator iter = ptables.begin(); iter != ptables.end(); ){
			if(select_operation[*iter] == NULL){
				iter++;
			}else{
				Relation *temp_relation;
				vector<Tuple> tuples  =  p->Singletable_Select( *iter  , select_operation[*iter] ) ;
				if(tuples.size() != 0){
					temp_relation = p->Create_Table( ( *iter) + "-SELECTION",  tuples) ;
				}else{
					vector<string> field_names = p->schema_manager.getRelation(*iter)->getSchema().getFieldNames(); 
					vector<enum FIELD_TYPE> field_types =  p->schema_manager.getRelation(*iter)->getSchema().getFieldTypes() ;
					temp_relation = p->Create_Table( (*iter) + "-SELECTION" , field_names, field_types ) ;
				}
				to_drop.push_back( temp_relation->getRelationName() ) ;
				iter = ptables.erase(iter) ;ptables.insert( iter, temp_relation->getRelationName() ) ;
			}
		}
		if(ptables.size() == 2){
			if(ptables[0] <= ptables[1]){
				ret = p->TwoPass_Join(ptables[0], ptables[1], commons ) ;
			}else{
				ret = p->TwoPass_Join(ptables[1], ptables[0], commons ) ;
			}
		}else{
			ret = p->Join_Table(ptables, commons) ;
		}
		for(vector<string>::iterator iter = to_drop.begin(); iter != to_drop.end(); iter++){
			p->Drop_Table(*iter) ;
		}
		if(output_s.empty() ){
		}else{
			string temp_table_name = "temp_table";
			while(p->schema_manager.relationExists(temp_table_name)) {
				temp_table_name += "-a";
			}
			p->Create_Table( temp_table_name, ret ) ;
			 tmp_relations.push_back(temp_table_name) ;

			ret = p->Singletable_Select(temp_table_name, output_s.top() ) ;
		}
	}else if(this->type == TABLE){
		if(table_name != NULL) { (*table_name ) = string( this->info[0] ) ;}
		ret = p->Singletable_Select(this->info[0], output_s.empty() ? NULL : output_s.top() );
	}else{
		return ret;
	}
	if(ret.size() != 0 && print){
		vector<string> field_names = 
			ret[0].getSchema( ).getFieldNames() ;
		cout <<  "-----------------" << endl ;
		output_stream.open("TinySQL_output.txt",ios::out | ios::app);
		output_stream << endl ;
		for(vector<string>::iterator iter = field_names.begin(); iter != field_names.end(); iter++){
			cout<< *iter << ' ' ;
			output_stream << *iter << ' ' ; 
		} 
		cout << endl << "-----------------" << endl ;
	    output_stream << endl << "-----------------" << endl ;
		for(vector<Tuple>::iterator iter = ret.begin(); iter != ret.end(); iter ++ ){
			cout << (*iter) << endl;
			output_stream << (*iter) << endl;
		}
		cout <<  "-----------------" << endl ;
		output_stream <<  "-----------------" << endl ;
		output_stream.close();
	}
	return  ret;
}
int main( int argc, char **argv ){
	
   // output_stream.open("TinySQL_output.txt",ios::trunc);
    output_stream.open("TinySQL_output.txt",ios::out);
    output_stream << "=================================" << endl << endl;
    output_stream.close();
    p = Operations::getInstance();
	
	/* For debug */
	#ifdef DEBUG
		error_output.rdbuf(std::cerr.rdbuf() );
	#else
		cerr.rdbuf(error_output.rdbuf() );
	#endif
	
	++argv, --argc;  /* skip over program name */
	if ( argc > 0 )
	{
		yyin = fopen( argv[0], "r" );
	}
	else
	{
		yyin = stdin;
	}
	yylex();
}
