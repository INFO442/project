/*
 * atom.h
 *
 *  Created on: May 17, 2018
 *      Author: yiranchen
 */

#ifndef ATOM_H_
#define ATOM_H_

#include <iostream>
#include <vector>
using namespace std;
class Atom{
public:
	//constructor from vector<int>
	Atom(vector<int>& line);
	//return all content
	vector<int> get_all() const;
	//return the element at index
	int get(int index)const;
	//get arity
	int get_arity() const;
	//print all content
	void print_line() const;
private:
	vector<int> line;
	int arity;
};





#endif /* ATOM_H_ */
