/*
 * join.cpp
 *
 *  Created on: 18 mai 2018
 *      Author: shancheng
 */
#include <stdio.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include "table.h"
#include "atom.h"
#include "join.h"

using namespace std;

Table Join::join(Table& T1, Table& T2, vector<vector<int> > x, int* dict_x) {
	int* dict = dict_x;
	//Assume that here t1 and t2 have been sorted with the same order for common elements in x
	//we have to add a sort process here
	int* dict_1 = Join::generate_dict(x[0], dict_x, T1.get_arity());
	int* dict_2 = Join::generate_dict(x[1], dict_x, T2.get_arity());
	T1.set_permut(dict_1, T1.get_arity());
	T2.set_permut(dict_2, T2.get_arity());
	T1.sort_permut();
	T2.sort_permut();

	vector<Atom>::const_iterator it1T1 = T1.get_content().begin();
	vector<Atom>::const_iterator it2T1 = T1.get_content().begin();

	vector<Atom>::const_iterator it1T2 = T2.get_content().begin();
	vector<Atom>::const_iterator it2T2 = T2.get_content().begin();

	int arity = T1.get_arity() + T2.get_arity() - x[0].size();
	Table table(arity);

	while (it1T1 != T1.get_content().end()) {
		if (it1T2 == T2.get_content().end())
			break;
		it2T1 = it1T1 + 1;
		it2T2 = it1T2 + 1;
		if (Atom::compare(*it1T1, *it1T2, x, dict) < 0) {
			it1T1++;
			if (it1T1 == T1.get_content().end())
				break;
			continue;
		}

		while (Atom::compare(*it1T1, *it1T2, x, dict) > 0) {
			it1T2++;
			it2T2 = it1T2 + 1;
			if (it1T2 == T2.get_content().end())
				break;
		}

		if (it1T2 == T2.get_content().end())
			break;

		if (Atom::compare(*it1T1, *it1T2, x, dict) == 0) {

			while (it2T1 != T1.get_content().end()
					&& Atom::compare_r(*it2T1, *it1T1, x[0], dict) == 0)
				it2T1++;
			while (it2T2 != T2.get_content().end()
					&& Atom::compare_r(*it2T2, *it1T2, x[1], dict) == 0)
				it2T2++;
			for (vector<Atom>::const_iterator it1 = it1T1; it1 != it2T1;
					it1++) {
				for (vector<Atom>::const_iterator it2 = it1T2; it2 != it2T2;
						it2++) {
					Atom tmp = Join::merge(*it1, *it2, x);
					table.add_line(tmp);
				}
			}
			it1T1 = it2T1;
			it1T2 = it2T2;
		}

	}

	return table;

}

Atom Join::merge(Atom a1, Atom a2, vector<vector<int> > common_x) {
	int arity = a1.get_arity() + a2.get_arity() - common_x[0].size();

	vector<int> line(a1.get_all());
	vector<int> a2line = a2.get_all();
	for (int i = 0; i < a2line.size(); i++) {
		if (find(common_x[1].begin(), common_x[1].end(), i)
				== common_x[1].end())
			line.push_back(a2line[i]);
	}
//	assert(line.size()==arity);
	Atom atom(line);
	return atom;
}
int* Join::generate_dict(vector<int> x, int* dict_x, int arity) {
	int nx = x.size();
	vector<int> dict_vect;
	int* dict = new int[arity];
	for (int i = 0; i < nx; i++) {
		dict_vect.push_back(x[dict_x[i]]);
	}
	for (int i = 0; i < arity; i++) {
		if (find(x.begin(), x.end(), i) == x.end())
			dict_vect.push_back(i);
	}
//	assert(arity==dict_vect.size());
	for (int i = 0; i < arity; i++) {
		dict[i] = dict_vect[i];
	}
	return dict;
}

