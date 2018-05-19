/*
 * atom.cpp
 *
 *  Created on: May 17, 2018
 *      Author: yiranchen
 */
#include <stdio.h>
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
int Atom::compare(const Atom& a, const Atom &b,const int *dict){
	int arity=a.get_arity();
	//Test if arities match with len(dict)
	if(arity!=b.get_arity()){
		printf("error:arities don't match:\n");
		return -2;
	}
	int index=0;
	//order
	for(int i=0;i<arity;i++){
		index=dict[i];
		if(a.get(index)<b.get(index)){
			return -1;
		}
		if(a.get(index)>b.get(index)){
			return 1;
		}
	}
// if a==b return false
	return 0;

}

int Atom::compare(const Atom& a, const Atom& b,const vector<vector<int> > common_x, int *dict_x){
	int n_common = common_x[0].size();
	for(int i=0;i<n_common;i++){
		int index = dict_x[i];

		if(a.get(common_x[0][index])<b.get(common_x[1][index])){
			return -1;
		}
		if(a.get(common_x[0][index])>b.get(common_x[1][index])){
					return 1;
		}
	}

	return 0;
}

int Atom::compare_r(const Atom& a, const Atom& b, const vector<int> restriction,int *dict_x){
	int n_common = restriction.size();
	for(int i=0;i<n_common;i++){
		int index = dict_x[i];
		if(a.get(restriction[index])<b.get(restriction[index])){
			return -1;
		}
		if(a.get(restriction[index])>b.get(restriction[index])){
			return 1;
		}
	}
	return 0;
}
