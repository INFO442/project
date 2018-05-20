/*
 * distribute.cpp
 *
 *  Created on: May 18, 2018
 *      Author: yiranchen
 */

//distribution can be further improved

#include <unordered_map>
#include <stdio.h>
#include <iostream>
#include <mpi.h>
#include<vector>
#include"join.h"
#include "table.h"
#include "atom.h"

#define ROOT 0
#define Buffer_Print_Size 600
#define TAG_RESULT 100



using namespace std;

void send(Table& t, int num_p, int tag, int& flag, int arity);
void receive(Table& t, int tag, int& flag, int arity);
void send_toRoot(Table& t, int root, int src, int arity);
void receive_inRoot(Table& t, int num_p, int arity);
int print_vector(vector<int>& v, char * buffer) ;
int print_table(Table& T, char * buffer);
unordered_map <int,int> hashfunction(Table &t,int name,int num_p);



int print_vector(vector<int>& v, char * buffer) {
	int size = v.size();
	int n = 0;
	for (int i = 0; i < size; i++) {
		n += sprintf(buffer + n, " %d ", v[i]);
	}
	n += sprintf(buffer + n, "\n");
	return n;
}
int print_table(Table& T, char * buffer) {
	vector<vector<int>> t=T.get_content();
	int size = t.size();
	int n = sprintf(buffer, "table is:\n");
	for (int i = 0; i < size; i++) {
		n += print_vector(t[i], buffer + n);
	}
	return n;
}

void send(Table& t, int num_p, int tag, int& flag, int name,
		int arity) {

	flag = 0;
	cout << "Number_Pro:" << num_p << endl;
	//construction of hashmap for balanced distribution
	unordered_map <int,int> m=hashfunction(t, name, num_p);

	//send data
	for (vector<Atom >::iterator itr = t.begin(); itr != t.end();
			itr++) {
		MPI_Request reqs;
		MPI_Status status;
		Atom atom = *itr;
		vector<int> a=atom.get_all();
		int destination = m[a[name]] ;
		cout << "Destination:" << destination << endl;
		if (destination == ROOT) {
			continue;
		}
		MPI_Isend(&a[0], arity, MPI_INT, destination, tag, MPI_COMM_WORLD,
				&reqs);
		MPI_Wait(&reqs, &status);
		t.erase(itr);
		itr--;
	}

	//return signal that sending finished
	for (int i = 0; i < num_p; i++) {
		MPI_Request reqs;
		MPI_Status status;
		vector<int> a(arity, -1);
		flag = 1;
		MPI_Isend(&a[0], arity, MPI_INT, i, tag, MPI_COMM_WORLD, &reqs);
		MPI_Wait(&reqs, &status);

	}
}

unordered_map <int,int> hashfunction(Table &t,int name,int num_p){
	unordered_map<int, int> m;
	int id_p=0;
	for(vector<Atom>::iterator itr=t.begin(); itr!=t.end();itr++){
		int key=itr->get(name);
		if(m.find(key)==m.end()){
			m[key]=id_p;
			id_p=(id_p+1)%num_p;
		}
	}
	return m;
}


void receive(Table& t, int tag, int& flag, int arity) {
	vector<int> atom(arity);
	MPI_Request reqs;
	MPI_Status status;
	while (atom[0] >= 0) {
		MPI_Irecv(&atom[0], arity, MPI_INT, ROOT, tag, MPI_COMM_WORLD, &reqs);
		MPI_Wait(&reqs, &status);
		if (atom[0] >= 0) {
			Atom a=Atom(atom);
			t.add_line(a);
		}
	}
}

void send_toRoot(Table& t, int root, int src, int arity) {

	MPI_Request reqs;
	MPI_Status status;
	int destination = root;
	//send data
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end();
			itr++) {

		Atom a = *itr;
		vector<int> atom=a.get_all();

		MPI_Isend(&atom[0], arity, MPI_INT, destination, TAG_RESULT + src,
				MPI_COMM_WORLD, &reqs);
		MPI_Wait(&reqs, &status);
	}

	//return signal that sending finished

	vector<int> atom(arity, -1);
	MPI_Isend(&atom[0], arity, MPI_INT, destination, TAG_RESULT + src,
			MPI_COMM_WORLD, &reqs);
	MPI_Wait(&reqs, &status);

}
void receive_inRoot(Table& t, int num_p, int arity) {
	MPI_Request reqs;
	MPI_Status status;
	for (int i = 1; i < num_p; i++) {
		vector<int> atom(arity);
		cout << "received:" << i << endl;
		while (atom[0] >= 0) {
			MPI_Irecv(&atom[0], arity, MPI_INT, i, TAG_RESULT + i, MPI_COMM_WORLD,
					&reqs);
			MPI_Wait(&reqs, &status);
			if (atom[0] >= 0) {
				Atom a=Atom(atom);
				t.add_line(a);
			}
		}
	}
}

int main(int argc, char *argv[]) {
	int id, num_p, flag, tag, num_table;
	MPI_Status status;
	MPI_Request reqs;
	// Initialize MPI.
	MPI_Init(&argc, &argv);
	// Get the number of processes.
	MPI_Comm_size(MPI_COMM_WORLD, &num_p);
	// Get the individual process ID.
	MPI_Comm_rank(MPI_COMM_WORLD, &id);

	int arity, name;
	//raw data
	Table t;
	//data result
//	vector<vector<int> > t_result(0);

//The master/root processor load data
	if (id == ROOT) {
//    	const char* file="twitter.dat.txt";
		int s1[] = { 1, 2, 3 };
		int s2[] = { 2, 3, 1 };
		int s3[] = { 3, 2, 1 };
		int s4[] = { 2, 1, 3 };
		int s5[] = { 5, 1, 3 };
		vector<int> v1(s1, s1 + 3);
		vector<int> v2(s2, s2 + 3);
		vector<int> v3(s3, s3 + 3);
		vector<int> v4(s4, s4 + 3);
		vector<int> v5(s5, s5 + 3);
		Atom a1=Atom(v1);
		Atom a2=Atom(v2);
		Atom a3=Atom(v3);
		Atom a4=Atom(v4);
		Atom a5=Atom(v5);
		Table* pt=new Table(a1.get_arity());
		t=*pt;
//		t.push_back(v1);
//		t.push_back(v2);
//		t.push_back(v3);
//		t.push_back(v4);
//		t.push_back(v5);
		t.add_line(a1);
		t.add_line(a2);
		t.add_line(a3);
		t.add_line(a4);
		t.add_line(a5);
		arity = t.get_arity();
		name = 0;
		num_table = 1;
	}

	//The master/root processor send arity to all the slave processor
	MPI_Bcast(&arity, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	MPI_Bcast(&name, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	MPI_Bcast(&num_table, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	//Root distribute data to every slave //Slave receive data
	flag = 0;
	tag = 0;
	if (id == ROOT) {
		send(t, num_p, tag, flag, name, arity);
	} else {
		receive(t, tag, flag, arity);
	}

	MPI_Barrier (MPI_COMM_WORLD);
	// joint raw data in each processors
	Table t_result=t;

	//  resending results of processors to root
	MPI_Barrier(MPI_COMM_WORLD);

	if (id != ROOT) {
		send_toRoot(t_result, ROOT, id, arity);
	} else {
		receive_inRoot(t_result, num_p, arity);
	}

	MPI_Barrier(MPI_COMM_WORLD);

	// Print off distribution result

	char buffer[Buffer_Print_Size];
	int n = sprintf(buffer, "Processeur ID:%d arity: %d\n Table:%d", id, arity,
			tag);
	n = print_table(t, buffer + n);
	if (id != ROOT) {
		MPI_Send(buffer, Buffer_Print_Size, MPI_CHAR, ROOT, 0, MPI_COMM_WORLD);
	}

	if (id == ROOT) {
		printf("\n%s", buffer);
		for (int i = 1; i < num_p; i++) {
			MPI_Recv(buffer, Buffer_Print_Size, MPI_CHAR, i, 0, MPI_COMM_WORLD,
					&status);
			printf("\n%s", buffer);
		}
	}

	// Print off joint result

	char bufferj[Buffer_Print_Size];
	int nj = sprintf(bufferj, "Processeur ID:%d arity: %d\n Table:result", id,
			arity);
	nj = print_table(t_result, bufferj + nj);

	if (id != ROOT) {
		MPI_Send(bufferj, Buffer_Print_Size, MPI_CHAR, ROOT, TAG_RESULT,
				MPI_COMM_WORLD);
	}
	if (id == ROOT) {
		printf("\n%s", bufferj);
		for (int i = 1; i < num_p; i++) {
			MPI_Recv(bufferj, Buffer_Print_Size, MPI_CHAR, i, TAG_RESULT,
					MPI_COMM_WORLD, &status);
			printf("\n%s", bufferj);
		}
	}

	// Terminate MPI.
	MPI_Finalize();

	return 0;

}
