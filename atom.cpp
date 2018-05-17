/*
 * atom.cpp
 *
 *  Created on: May 17, 2018
 *      Author: yiranchen
 */
#include <iostream>
#include <vector>
#include "atom.h"
using namespace std;

Atom::Atom(vector<int>& line): line(line),arity(line.size())
{}
vector<int> Atom::get_all() const{
	return this->line;
}

int Atom::get(int index) const{
	if(index >=this->arity){
		cout<<"error:index>=arity"<<endl;
		return -1;
	}
	return this->line[index];
}
int Atom::get_arity() const{
	return this->arity;
}
void Atom::print_line() const{
		for(vector<int>::const_iterator itr=this->line.begin();itr!=this->line.end();++itr){
			printf(" %d ",*itr);
		}
		printf("\n");
}

