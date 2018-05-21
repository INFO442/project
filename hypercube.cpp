/*
 * hypercube.cpp
 *
 *  Created on: 20 mai 2018
 *      Author: shancheng
 */

#include<map>
#include <unordered_map>
#include <stdio.h>
#include <iostream>
#include <mpi.h>
#include <math.h>
#include<vector>
#include"join.h"
#include "table.h"
#include "atom.h"

#define ROOT 0
#define Buffer_Print_Size 600
#define TAG_RESULT 100

using namespace std;

void send_toRoot(Table& t, int root, int src);
void receive_inRoot(Table& t, int num_p);

int print_vector(vector<int>& v, char * buffer);
int print_table(Table& T, char * buffer);

map<int, int> hashfunction(Table &t, Table &t2, int name1, int name2,
		int num_p);
int compute_proc(int* hashcode, int* total, int size); //this function is going to compute which process to put the atom with this hashcode

int compute_proc(int* hashcode, int m_p, int size) {
	int n = 0;
	for (int i = 0; i < size; i++) {
		n = n * m_p + hashcode[i];
	}
	return n;
}

void DistributeMultiDim(Table& localT, Table& T, map<int, int> hash, int m_p,
		int id, int*com_variable) {
	for (vector<Atom>::iterator it = T.begin(); it != T.end(); it++) {
		int hcode[3];
		int notinclude;
		for (int i = 0; i < 3; i++) {
			if (com_variable[i] < 0) {
				hcode[i] = -1;
				notinclude = i;
			} else {
				hcode[i] = hash[it->get(com_variable[i])];
			}
		}
		for (int i = 0; i < m_p; i++) {
			hcode[notinclude] = i;
			if (compute_proc(hcode, m_p, 3) == id) {
				Atom a = *it;
				localT.add_line(a);
				break;
			}
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
		while (atom[0] >= 0) {
			MPI_Irecv(&atom[0], arity, MPI_INT, i, TAG_RESULT, MPI_COMM_WORLD,
					&reqs);
			MPI_Wait(&reqs, &status);
			if (atom[0] >= 0) {
				Atom a = Atom(atom);
				t.add_line(a);
			}
		}
		cout << "received:" << i << endl;
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
	//        vector<int> atomv(arity, 0);
	//cout << "reception start:" << src << endl;

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

	//cout << "reception end :" << src << endl;
	delete atom;

}
int main(int argc, char *argv[]) {

	int id, num_p, m_p, flag, tag, num_table;

	MPI_Status status;
	MPI_Request reqs;
	// Initialize MPI.
	MPI_Init(&argc, &argv);
	// Get the number of processes.
	MPI_Comm_size(MPI_COMM_WORLD, &num_p);
	// Get the individual process ID.
	MPI_Comm_rank(MPI_COMM_WORLD, &id);
	double start, end;

	m_p = (int) pow(num_p, 1 / 3.0); //num_p= m_p*m_p*m_p
	int arity; //Arity of the table
	if (id == ROOT) {
		cout << "m_p:" << m_p << endl;
		start = MPI_Wtime();
	}
	const char* file = "facebook.dat";
	const char* file2 = "facebook.dat";
	const char* file3 = "facebook.dat";
	Table T1 = Table(file); //T(X1,X2)
	Table T2 = Table(file2); //T(X2,X3)
	Table T3 = Table(file3); //T(X1,X3)

	arity = T1.get_arity();

	int common_x_T1[3] = { 0, 1, -1 }; //T1(X1,X3)
	int common_x_T2[3] = { -1, 0, 1 }; //T2(X2,X3)
	int common_x_T3[3] = { 0, -1, 1 }; //T3(X1,X3)

	num_table = 3;
	Table localT1 = Table(arity);
	Table localT2 = Table(arity);
	Table localT3 = Table(arity); //These are three local tables in each process

	//Root distribute data to every slave
	flag = 0;
	tag = 0;
	int name1 = 1;
	int name2 = 0;
	map<int, int> hash = hashfunction(T1, T2, name1, name2, m_p); //compute the hashfunction according to the distribution of data

//	Each process decide its own localtables
	DistributeMultiDim(localT1, T1, hash, m_p, id, common_x_T1);
	DistributeMultiDim(localT2, T2, hash, m_p, id, common_x_T2);
	DistributeMultiDim(localT3, T3, hash, m_p, id, common_x_T3);

	cout << "Id " << id << "distributedT1 size:" << localT1.get_size()
			<< " T2 size:" << localT2.get_size() << " T3 size:"
			<< localT3.get_size() << endl;

//	 joint raw data in each processors

	//First Joining
	vector < vector<int> > common_T1T2;
	vector<int> v1(1, 1);
	vector<int> v2(1, 0);
	common_T1T2.push_back(v1);
	common_T1T2.push_back(v2);
	int dict_x1[] = { 0 };
	Table T1T2 = Join::join(localT1, localT2, common_T1T2, dict_x1);

	//second Joining
	vector < vector<int> > common_T1T2T3;
	vector<int> v3(2, 0);
	v3[1] = 2;
	vector<int> v4(2, 0);
	v4[1] = 1;
	common_T1T2T3.push_back(v3);
	common_T1T2T3.push_back(v4);
	int dict_x2[] = { 0, 1 };
	cout << "here is ok" << endl;

	Table T1T2T3 = Join::join(T1T2, localT3, common_T1T2T3, dict_x2);

	cout << "id:" << id << " result_arity: " << T1T2T3.get_arity() << endl;
	cout << "id:" << id << " result_size: " << T1T2T3.get_size() << endl;

	MPI_Barrier (MPI_COMM_WORLD);

//	  resending results of processors to root

//
	for (int i = 1; i < num_p; i++) {
		MPI_Barrier(MPI_COMM_WORLD);
		if (id != ROOT) {
			if (id == i)
				send_toRoot(T1T2T3, ROOT, id);
		} else {
			receive_inRoot_single(T1T2T3, i);
		}
	}

// Terminate MPI.

	if (id == ROOT) {
		end = MPI_Wtime();
		cout << "Processing time:" << (end - start) << endl;
	}
	if (id == ROOT) {
		cout << "id:" << id << " result_arity: " << T1T2T3.get_arity() << endl;
		cout << "id:" << id << " result_size: " << T1T2T3.get_size() << endl;

	}

	MPI_Finalize();

	return 0;

}

