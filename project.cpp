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
void testTable(){
	int n1[]={1,2,3};
		int n2[]={2,3,1};
		int n3[]={1,3,2};
		int n4[]={3,1,2};
		vector<int> v1(n1,n1+3);
		vector<int> v2(n2,n2+3);
		vector <int> v3(n3,n3+3);
		vector <int> v4(n4,n4+3);
	//	v2[1]=2;
	//	v3[1]=3;
	//	v4[1]=4;
		Atom a1=Atom(v1);
		Atom a2=Atom(v2);
		Atom a3=Atom(v3);
		Atom a4=Atom(v4);
		Table t=Table(3);
		t.add_line(a2);
		t.add_line(a1);
		t.add_line(a3);
		t.add_line(a4);
		int d[]={1,0,2};
		t.set_permut(d,3);

		cout<<t.compare(a3, a4)<<endl;
		t.sort_permut();
		t.print_head(4);
		t.print_permut();

	//	const char* file="/Users/yiranchen/Documents/studyinX/info/info442/project/src/twitter.dat.txt";
	//	Table t(file);
	//	printf ("arity:%d\n",t.get_arity());
	//	printf ("size:%d\n",t.get_size());
	//	cout<<"print_line";
	//	t.sort_table();
	//	t.print_head(50);
}
int main(int argc,char **argv) {
	testTable();

}
