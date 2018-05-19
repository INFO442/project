/*
 * join.h
 *
 *  Created on: 18 mai 2018
 *      Author: shancheng
 */

#ifndef JOIN_H_
#define JOIN_H_

#include <iostream>
#include <vector>
#include <algorithm>

#include "table.h"
#include "atom.h"
class Join {
public:
	static Table join(Table& T1,Table& T2,vector<vector<int> > common_x, int* dict_x);
	vector<vector<int> > common_index(void);
	static Atom merge(Atom a1, Atom a2, vector<vector<int> > common_x);
	static int* generate_dict(vector<int> x, int* dict_x, int arity);

private:

};

#endif /* JOIN_H_ */
