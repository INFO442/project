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
#include "join.h"
using namespace std;
void testTable() {
	int n1[] = { 1, 2, 3 };
	int n2[] = { 2, 3, 1 };
	int n3[] = { 1, 3, 2 };
	int n4[] = { 3, 1, 2 };
	vector<int> v1(n1, n1 + 3);
	vector<int> v2(n2, n2 + 3);
	vector<int> v3(n3, n3 + 3);
	vector<int> v4(n4, n4 + 3);
	//	v2[1]=2;
	//	v3[1]=3;
	//	v4[1]=4;
	Atom a1 = Atom(v1);
	Atom a2 = Atom(v2);
	Atom a3 = Atom(v3);
	Atom a4 = Atom(v4);
	Table t = Table(3);
	t.add_line(a2);
	t.add_line(a1);
	t.add_line(a3);
	t.add_line(a4);
	int d[] = { 1, 0, 2 };
	t.set_permut(d, 3);

	cout << t.compare(a3, a4) << endl;
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
int main(int argc, char **argv) {
	//testTable();
	Table t1 = Table(2);
	Table t2 = Table(2);
	Table t3 = Table(2);
	int s1[] = { 1, 2 };
	int s12[] ={2,2};
	int s2[] = { 2, 3 };
	int s3[] = { 2, 3 };
	vector<int> v1(s1, s1 + 2);
	vector<int> v12(s12,s12+2);
	vector<int> v2(s2, s2 + 2);
	vector<int> v3(s3, s3 + 2);

	Atom a1 = Atom(v1);
	Atom a12 = Atom(v12);
	Atom a2 = Atom(v2);
	Atom a3 = Atom(v3);

	t1.add_line(a1);
	t1.add_line(a12);
	t2.add_line(a2);
	t3.add_line(a3);
	vector<vector<int> > common_x1;
	vector<int> x0(1, 1);
	vector<int> x1(1, 0);

	common_x1.push_back(x0);
	common_x1.push_back(x1);
	int dict_x[1] = { 0 };


	Table t = Join::join(t1,t2,common_x1, dict_x);
	cout << t.get_size()<<endl;
	t.print_head(2);
	cout<<"here is ok"<<endl;
	vector<vector<int> > common_x2;
	vector<int> x2(2, 0);
	x2[1] = 2;
	vector<int> x3(2, 0);
	x3[1] = 1;
	common_x2.push_back(x2);
	common_x2.push_back(x3);
	int dict_x2[2] = { 0, 1 };

	Table res = Join::join(t,t3,common_x2, dict_x2);
	cout << res.get_size();
	res.print_head(2);

}
