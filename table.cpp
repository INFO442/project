#include "table.h"
#include <iostream>
#include <vector>
#include "atom.h"
using namespace std;

Table::Table(int arity){
	this->arity=arity;
	this->content=vector<Atom>();
	this->size=0;
}
Table::Table(int arity,int size){
	this->arity=arity;
	this->content=vector<Atom>();
	this->size=0;
}
void Table::add_line(Atom& line){
	this->content.push_back(line);
	this->size++;
}
Atom Table::remove_end_line(){
	if(this->size==0){
		printf("error:size==0");
		vector<int> v;


		Atom a(v);
		return a;
	}

	Atom a=content.back();
	this->content.pop_back();
	this->size--;
	return a;
}
void Table::print_head()const{
	if(this->size==0){
		printf("empty!\n");
		return;
	}
	printf("the table is:\n");
	for(vector<Atom>::const_iterator itr=this->content.begin();itr!=content.end();++itr){

		itr->print_line();
	}
}

