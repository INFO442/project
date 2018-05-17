/*
 * table.h
 *
 *  Created on: May 17, 2018
 *      Author: yiranchen
 */
// STL includes
#include <iostream>
#include <vector>

#ifndef TABLE_H_
#define TABLE_H_
using namespace std;
class Table{

public:

	//constructor
	Table(int arity);
	Table(int arity,int size);
	// add and remove lines
	void add_line(vector<vector<int>> line);
	vector<vector<int>> remove_end_line();
	vector<vector<int>> remove_line(int index);

private:
	int arity;
	int size;
	vector<vector<int>> content;


};







#endif /* TABLE_H_ */
