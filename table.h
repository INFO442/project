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
	Table(const char* file);

	//destructor
	~Table();


	// add and remove lines
	void add_line(Atom& line);
	Atom* remove_end_line();





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

	//sorting function using this->dict as permutation sorting lexicograph
		void sort_permut()
			{	// create a sortstruct and pass it to std::sort
				sortstruct s(this);
				::sort (this->content.begin(), this->content.end (),s);
			}

//get information of table
	void print_permut()const;
	int get_arity() const;
	int get_size() const;
//get element
	Atom* get(int index) const;
//print_head
	void print_head(int num_line) const;


	//using permutation as lexicograph to compare
	//-2:error
	//-1 a<b
	//+1 a>b
	//0 a==b

	//using  this->dict as defaut lexicograph
	int compare(const Atom& a,const Atom& b) const;

	//using given permut as lexicograph
	int compare(const Atom& a,const Atom& b,int *permute) const;
	//seting dict (default lexicograph as well as sorting lexicograph)
	void set_permut(int *d,int len);




private:
//	bool compare(const Atom& a,const Atom& b);
	int arity;
	int size;
	int *dict;
	vector<Atom> content;


};







#endif /* TABLE_H_ */
