/*
 * distribute.cpp
 *
 *  Created on: May 18, 2018
 *      Author: yiranchen
 */

//distribution can be further improved
#include<map>
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

void send(Table& t, map<int, int> m, int num_p, int tag, int& flag, int arity);
void receive(Table& t, int tag, int& flag, int arity);
void send_toRoot(Table& t, int root, int src);
void receive_inRoot(Table& t, int num_p);
int print_vector(vector<int>& v, char * buffer);
int print_table(Table& T, char * buffer);
map<int, int> hashfunction(Table &t, Table &t2, int name1, int name2,
		int num_p);
void distri_print(Table& t, string& name_Table, int id, int arity, int num_p);
void print_bug_result(int id, Table& t);
void print_bug(int id, Table& t, Table& t2);

void print_bug_result(int id, Table& t, bool info) {
	cout << "ID:" << id << " T_result:" << endl;
	if (info) {
		t.print_head(t.get_size());
	}
	cout << "size: " << t.get_size() << " arity: " << t.get_arity() << endl;
}
void print_bug(int id, Table& t, Table& t2) {
	cout << "ID:" << id << " T1:" << endl;

	t.print_head(t.get_size());
	cout << "size: " << t.get_size() << " arity: " << t.get_arity() << endl;

	cout << "ID:" << id << " T2:" << endl;

	t2.print_head(t2.get_size());
	cout << "size: " << t2.get_size() << " arity: " << t2.get_arity() << endl;
}

void distribute(Table& t, map<int, int> m, int id, int name) {

	for (vector<Atom>::iterator itr = t.begin(); itr != t.end();) {
		int key = itr->get(name);
		// delete if m[key]!=id
		if (m[key] != id) {
			t.erase(itr);

		} else {
			itr++;
		}

	}
}
void distribute_by_copy(Table& t, Table& t_, map<int, int> m, int id,
		int name) {
	for (vector<Atom>::iterator itr = t_.begin(); itr != t_.end(); itr++) {
		int key = itr->get(name);
		//copy into t for t_ if m(key)==id
		if (m[key] == id) {
			Atom a = *itr;
			t.add_line(a);
		}

	}
}

void distri_print(Table& t, string name_Table, int id, int arity, int num_p) {
	MPI_Status status1;
	char bufferj[Buffer_Print_Size];
	int nj = sprintf(bufferj, "Processeur ID:%d arity: %d\n Table:%s", id,
			arity, name_Table.c_str());
	nj = print_table(t, bufferj + nj);

	if (id != ROOT) {
		MPI_Send(bufferj, Buffer_Print_Size, MPI_CHAR, ROOT, TAG_RESULT,
				MPI_COMM_WORLD);
	}
	if (id == ROOT) {
		printf("\n%s", bufferj);
		for (int i = 1; i < num_p; i++) {
			MPI_Recv(bufferj, Buffer_Print_Size, MPI_CHAR, i, TAG_RESULT,
					MPI_COMM_WORLD, &status1);
			printf("\n%s", bufferj);
		}
	}

}

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
	vector<Atom> t = T.get_content();
	int size = t.size();
	int n = sprintf(buffer, "table is:\n");
	for (int i = 0; i < size; i++) {
		vector<int> v = t[i].get_all();
		n += print_vector(v, buffer + n);
	}
	return n;
}

void send(Table& t, map<int, int> m, int num_p, int tag, int& flag, int name,
		int arity) {

	flag = 0;
	cout << "Number_Pro:" << num_p << endl;
	//construction of hashmap for balanced distribution

	//send data
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		MPI_Request reqs;
		MPI_Status status;
		Atom atom = *itr;
		vector<int> a = atom.get_all();
		int destination = m[a[name]];
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

map<int, int> hashfunction(Table &t, Table &t2, int name1, int name2,
		int num_p) {
	map<int, int> m;
	int id_p = 0;
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		int key = itr->get(name1);
		if (m.find(key) == m.end()) {
			m[key] = id_p;
			id_p = (id_p + 1) % num_p;
		}
	}
	for (vector<Atom>::iterator itr = t2.begin(); itr != t2.end(); itr++) {
		int key = itr->get(name2);
		if (m.find(key) == m.end()) {
			m[key] = id_p;
			id_p = (id_p + 1) % num_p;
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
			Atom a = Atom(atom);
			t.add_line(a);
		}
	}
}

void send_toRoot(Table& t, int root, int src) {
	int arity = t.get_arity();
	MPI_Status status;
	int destination = root;
	//send data
	cout << "sending start:" << src << endl;
	int *atom = new int[arity];
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {

		Atom a = *itr;
		vector<int> atomv = a.get_all();

		for (int i = 0; i < atomv.size(); i++) {
			atom[i] = atomv[i];
		}

		MPI_Send(atom, arity, MPI_INT, destination, TAG_RESULT + src,
				MPI_COMM_WORLD);

	}

	//return signal that sending finished
	vector<int> atomv(arity, -1);
	for (int i = 0; i < arity; i++) {
		atom[i] = -1;

	}
	MPI_Send(atom, arity, MPI_INT, destination, TAG_RESULT + src,
			MPI_COMM_WORLD);
	cout << "sending end:" << src << endl;
	delete atom;
}
void receive_inRoot(Table& t, int num_p) {
	MPI_Request reqs;
	MPI_Status status;
	int arity = t.get_arity();
	for (int i = 1; i < num_p; i++) {
		vector<int> atom(arity, 0);
		cout << "reception start:" << i << endl;
		int step = 0;
		while (atom[0] >= 0) {

			MPI_Recv(&atom[0], arity, MPI_INT, i, TAG_RESULT + i,
					MPI_COMM_WORLD, &status);
//			MPI_Wait(&reqs, &status);
			if (atom[0] >= 0) {
				Atom a = Atom(atom);
				t.add_line(a);
			}
		}
		cout << "reception end :" << i << endl;
	}
}

void receive_inRoot_single(Table& t, int src) {

	MPI_Status status;
	int arity = t.get_arity();
	int* atom = new int[arity];
	vector<int> atomv(arity);

	for (int i = 0; i < arity; i++) {
		atom[i] = 0;
	}
//		vector<int> atomv(arity, 0);
	cout << "reception start:" << src << endl;

	while (atom[0] >= 0) {

		MPI_Recv(atom, arity, MPI_INT, src, TAG_RESULT + src, MPI_COMM_WORLD,
				&status);

		if (atom[0] >= 0) {

			for (int i = 0; i < arity; i++) {
				atomv[i] = atom[i];
			}

			Atom a = Atom(atomv);
			t.add_line(a);
		}

	}

	cout << "reception end :" << src << endl;
	delete atom;

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
	double start, end;
	int arity, arity2;

//The master/root processor load data
	if (id == ROOT) {
		start = MPI_Wtime();
	}
	const char* file = "facebook.dat.txt";
	const char* file2 = "dblp.dat.txt";
	Table t_ = Table(file);
	Table t2_ = Table(file2);
	arity = t_.get_arity();
	arity2 = t2_.get_arity();
	num_table = 2;
	Table t = Table(arity);
	Table t2 = Table(arity2);

	//Root distribute data to every slave //Slave receive data
	flag = 0;
	tag = 0;
	int name1 = 1;
	int name2 = 0;
	map<int, int> m = hashfunction(t_, t2_, name1, name2, num_p);

//	smarter way for distribution

	distribute_by_copy(t, t_, m, id, name1);
	distribute_by_copy(t2, t2_, m, id, name2);
	MPI_Barrier (MPI_COMM_WORLD);
	cout << "distributed_T1 size:" << t.get_size() << " T2 size:"
			<< t2.get_size() << endl;


//	 joint raw data in each processors

	vector<vector<int> > common_x(0);
	vector<int> v1_(1, 1);
	vector<int> v2_(1, 0);
	common_x.push_back(v1_);
	common_x.push_back(v2_);
	int dict_x[] = { 0 };
	Table t_result = Join::join(t, t2, common_x, dict_x);
	cout << "id:" << id << " result_arity: " << t_result.get_arity() << endl;
	cout << "id:" << id << " result_size: " << t_result.get_size() << endl;

	MPI_Barrier(MPI_COMM_WORLD);

//	  resending results of processors to root

//
	for (int i = 1; i < num_p; i++) {
		MPI_Barrier(MPI_COMM_WORLD);
		if (id != ROOT) {
			if (id == i)
				send_toRoot(t_result, ROOT, id);
		} else {
			receive_inRoot_single(t_result, i);
		}
	}

// Print off distribution result
	MPI_Barrier(MPI_COMM_WORLD);
	if (id == ROOT) {
		print_bug_result(ROOT, t_result,true);
	}

// Terminate MPI.

	if (id == ROOT) {
		end = MPI_Wtime();
		cout << "Processing time:" << (end - start) << endl;
	}

	MPI_Finalize();

	return 0;

}

