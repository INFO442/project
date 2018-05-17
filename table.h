/*
 * table.h
 *
 *  Created on: May 17, 2018
 *      Author: yiranchen
 */
// STL includes


#ifndef TABLE_H_
#define TABLE_H_

#include <iostream>
#include <vector>
#include "atom.h"
using namespace std;
class Table{

public:

	//constructor
	Table(int arity);
	Table(int arity,int size);

	// add and remove lines
	void add_line(Atom& line);
	Atom remove_end_line();
//	Atom remove_line(int index);
	//get element
	Atom get(int index) const;
	//print_head
	void print_head() const;
private:
	int arity;
	int size;
	vector<Atom> content;


};







#endif /* TABLE_H_ */
