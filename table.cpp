#include "table.h"
#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include "atom.h"
#include <stdio.h>
using namespace std;
//constructor
Table::Table(const char* file) {
	ifstream infile;
	infile.open(file);
	vector<Atom> content;
	int arity = 0;
	int size = 0;

	string line;

	bool isfirstline = true; // using it to make sure that we compute arity only with the fist line
	int n = 100000000;
	while (getline(infile, line) && (n--) > 0) {
		vector<int> tmp;
		stringstream ss;
		int localarity = 0; // the number of entries in this line
		ss << line;
		int entry;
		while (ss >> entry) {
			tmp.push_back(entry);
			localarity++;
		}

		if (isfirstline) {
			arity = localarity;
			isfirstline = false;
		} else {
			if (localarity != arity) {
				cout
						<< "Error format of Data: this data has different lines with different arities!!"
						<< endl;
				break;
			}
		}
		Atom atom(tmp);
		content.push_back(atom);
		size++;
	}
	this->content = content;
	this->arity = arity;
	this->size = size;
	infile.close();

	//constructing dictionnary,default value is set to {1,2,3,4,..,r}

	this->dict = new int[this->arity];
	for (int i = 0; i < (this->arity); i++) {
		dict[i] = i;
	}

}

Table::Table(int arity) {
	this->arity = arity;
	this->content = vector<Atom>();
	this->size = 0;
	//constructing dictionnary,default value is set to {1,2,3,4,..,r}
	this->dict = new int[this->arity];
	for (int i = 0; i < (this->arity); i++) {
		dict[i] = i;
	}

}
Table::Table(int arity, int size) {
	this->arity = arity;
	this->content = vector<Atom>();
	this->size = 0;

	//constructing dictionnary,default value is set to {1,2,3,4,..,r}
	this->dict = new int[this->arity];
	for (int i = 0; i < (this->arity); i++) {
		dict[i] = i;
	}
}
Table::Table(vector<vector<int> >& v) {
	this->arity = v[0].size();
	this->content = vector<Atom>();
	this->size = v.size();


	//constructing dictionnary,default value is set to {1,2,3,4,..,r}
	this->dict = new int[this->arity];
	for (int i = 0; i < (this->arity); i++) {
		dict[i] = i;
	}

	for (vector<vector<int> >::const_iterator itr = v.begin(); itr != v.end();
			++itr) {
		vector<int> v=*itr;
		Atom  a=Atom (v);
		content.push_back(a);
	}
}
Table::Table(vector<Atom>& v):content(v){
	this->arity=v[0].get_arity();
	this->size=v.size();
	this->dict = new int[this->arity];
		for (int i = 0; i < (this->arity); i++) {
			dict[i] = i;
		}
}
//destructor
Table::~Table() {
//	delete this->dict;
//	cout<<"!!!table destructed!\n"<<endl;
}

//add line
void Table::add_line(Atom& line) {
	if (line.get_arity() != this->arity) {
		printf("error:arity error\n");
		return;
	}
	this->content.push_back(line);
	this->size++;
}
Atom* Table::remove_end_line() {
	if (this->size == 0) {
		printf("error:size==0");
		return 0;
	}

	Atom a = content.back();
	this->content.pop_back();
	this->size--;
	vector<int> v = a.get_all();

	return new Atom(v);

}
void Table::print_head(int num_line) const {
	if (this->size == 0) {
		printf("empty!\n");
		return;
	}
	printf("the table is:\n");
	int i = 0;
	for (vector<Atom>::const_iterator itr = this->content.begin();
			itr != content.end(); ++itr) {
		if (i++ >= num_line) {
			return;
		}
		itr->print_line();
	}

}
// get arity, get by index get size
int Table::get_arity() const {
	return this->arity;
}
int Table::get_size() const {
	return this->size;
}

const vector<Atom>& Table::get_content() const {
	return this->content;
}
Atom* Table::get(int index) const {
	if (index >= this->size) {
		printf("error:size==0");
		return 0;
	}
	Atom a = this->content[index];

	vector<int> v = a.get_all();

	return new Atom(v);
}

//compare
int Table::compare(const Atom&a, const Atom &b) const {

	return Atom::compare(a, b, this->dict);
}

//print permut
void Table::print_permut() const {
	printf("the permutation of this table is:\n");
	for (int i = 0; i < this->arity; i++) {
		printf(" %d ", this->dict[i]);
	}
	printf("\n");
}
//set permut
void Table::set_permut(int *d, int len) {
	if (len != this->arity) {
		printf("permutation cannot match with arity\n");
		return;
	}
	for (int i = 0; i < this->arity; i++) {
		this->dict[i] = d[i];
	}
}

//void Table::sort_table( ){
//	auto f=[](Atom a,Atom b){
//		return Atom::compare(a,b,this->dict);
//	};
////	FredMemFn func = &Table::compare;
////
////	sort(this->content.begin(),this->content.end(),this->*func);
//}
