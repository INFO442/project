/*
 * table.h
 *
 *  Created on: May 17, 2018
 *      Author: yiranchen
 */
// STL includes


#ifndef TABLE_H_
#define TABLE_H_
#include <algorithm>
#include <iostream>
#include <vector>
#include "atom.h"
using namespace std;
class Table{

public:

	//constructor
	Table(int arity);
	Table(int arity,int size);
	Table(const char* file);
	Table(vector<vector<int> >& v);
	Table(vector<Atom >& v);
	//destructor
	~Table();
	// add and remove lines
	void add_line(Atom& line);
	Atom* remove_end_line();
//	Atom remove_line(int index);
	//get element
	Atom* get(int index) const;
	//print_head
	void print_head(int num_line) const;
	//sort table
//	void sort_table();
	//get arity

	//sort
	struct sortstruct
		{
			// sortstruct needs to know its containing object
			Table* t;
			sortstruct(Table* p) : t(p) {};

			// this is our sort function, which makes use compare of instance
			bool operator() ( Atom a, Atom b )
			{
				if(t->compare(a, b)>=0){
					return false;
				}
				return true;
			}
		};

	int get_arity() const;
	int get_size() const;
	int compare(const Atom& a,const Atom& b) const;
	void print_permut()const;
	void set_permut(int *d,int len);
	void sort_permut()
		{	// create a sortstruct and pass it to std::sort
			sortstruct s(this);
			::sort (this->content.begin(), this->content.end (),s);
		}
	//line iterator of Table
	const vector<Atom>& get_content() const;

	vector<Atom>::iterator begin(){
		return this->content.begin();
	}
	vector<Atom>::iterator end(){
		return this->content.end();
	}
	void erase(vector<Atom>::iterator itr){
		this->content.erase(itr);
	}


private:
//	bool compare(const Atom& a,const Atom& b);
	int arity;
	int size;
	int *dict;
	vector<Atom> content;


};


#endif /* TABLE_H_ */
