//============================================================================
// Name        : project.cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================
#include <iostream>
#include <vector>
#include "atom.h"
#include "table.h"
using namespace std;

int main(int argc,char **argv) {
//	vector<int> v1(3,1);
//	vector<int> v2(3,2);
//	vector <int> v3(3,10);
//	vector <int> v4(3,9);
//	Atom a1=Atom(v1);
//	Atom a2=Atom(v2);
//	Atom a3=Atom(v3);
//	Atom a4=Atom(v4);
//	Table t=Table(3);
//	t.add_line(a2);
//	t.add_line(a1);
//	t.add_line(a3);
//	t.add_line(a4);
//	t.sort_table();
	const char* file="/Users/yiranchen/Documents/studyinX/info/info442/project/src/twitter.dat.txt";
	Table t(file);
	printf ("arity:%d\n",t.get_arity());
	printf ("size:%d\n",t.get_size());
	cout<<"print_line";
	t.sort_table();
	t.print_head(50);


	return 0;
}
