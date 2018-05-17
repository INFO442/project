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

int main() {
	vector<int> v1(3,1);
	vector<int> v2(3,2);
	Atom a1=Atom(v1);
	Atom a2=Atom(v2);
	Table t=Table(3);
	t.add_line(a1);
	t.add_line(a2);
	t.print_head();
//	a.print_line();
//	printf ("arity:%d",a.get(1)) ;
//	cout << "!!!Hello World!!!" << endl; // prints !!!Hello World!!!
	return 0;
}
