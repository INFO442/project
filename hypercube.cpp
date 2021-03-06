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
#define TAG_RESULT 420

using namespace std;



int print_vector(vector<int>& v, char * buffer);
int print_table(Table& T, char * buffer);

//produce hashfunction according to the values in the given relations
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
//Distribute tuples to different process according to the hash function
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

void send_all_toRoot(Table& t, int root, int src) {
	int arity = t.get_arity();
	int table_size = t.get_size();
	MPI_Status status;
	int destination = root;
	//send data
// 	cout << "sending start:" << src << endl;
	MPI_Send(&table_size,1, MPI_INT, destination, TAG_RESULT + src,
			MPI_COMM_WORLD);
	int *all = new int[arity*table_size];
	int ni =0;
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		Atom a = *itr;
		vector<int> atomv = a.get_all();

		for (int i = 0; i < atomv.size(); i++) {
			all[ni++] = atomv[i];
		}
	}
	//assert(ni==arity*table_size);
        MPI_Send(all, arity*table_size, MPI_INT, destination, TAG_RESULT + src,MPI_COMM_WORLD);

	//return signal that sending finished
	
	
// 	cout << "sending end:" << src << endl;
	delete[] all;
}

void receive_all_inRoot(Table& t, int src) {

	MPI_Status status;
	int arity = t.get_arity();
	int table_size;
	
	

	//        vector<int> atomv(arity, 0);
	//cout << "reception start:" << src << endl;
	MPI_Recv(&table_size,1, MPI_INT, src, TAG_RESULT + src, MPI_COMM_WORLD,&status);
	int* all = new int[arity*table_size];
	
	MPI_Recv(all,arity*table_size, MPI_INT, src, TAG_RESULT + src, MPI_COMM_WORLD,&status);
	
	//write into table
	vector<int> atomv(arity);
	for(int i=0;i<table_size;i++){
	    for (int j = 0; j < arity; j++) {
		atomv[j] = all[i*arity+j];
	      }
	    Atom a = Atom(atomv);
	    t.add_line(a);
	}
	//cout << "reception end :" << src << endl;
	delete[] all;

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

	m_p = pow((double)num_p+0.0001, 1.0/3.0); //num_p= m_p*m_p*m_p
	int arity; //Arity of the table
	if (id == ROOT) {
		cout <<"num_p: "<<num_p<< " m_p:" << m_p << endl;
		start = MPI_Wtime();
	}
	//"twitter.dat" "facebook.dat"
	char s[] = "facebook.dat";
	const char* file = s;
	const char* file2 =s;
	const char* file3 = s;
	Table T1 = Table(file); //T(X1,X2)
	Table T2 = Table(file2); //T(X2,X3)
	Table T3 = Table(file3); //T(X1,X3)
	if(id ==ROOT){
	cout<<"database: "<<s<<endl;}
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

// 	cout << "Id " << id << "distributedT1 size:" << localT1.get_size()
// 			<< " T2 size:" << localT2.get_size() << " T3 size:"
// 			<< localT3.get_size() << endl;

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
	//cout << "here is ok" << endl;

	Table T1T2T3 = Join::join(T1T2, localT3, common_T1T2T3, dict_x2);

// 	cout << "id:" << id << " result_arity: " << T1T2T3.get_arity() << endl;
// 	cout << "id:" << id << " result_size: " << T1T2T3.get_size() << endl;

	MPI_Barrier (MPI_COMM_WORLD);

//	  resending results of processors to root

//
	for (int i = 1; i < num_p; i++) {
		MPI_Barrier(MPI_COMM_WORLD);
		if (id != ROOT) {
			if (id == i)
				send_all_toRoot(T1T2T3, ROOT, id);
		} else {
			receive_all_inRoot(T1T2T3, i);
		}
	}

// Terminate MPI.

	if (id == ROOT) {
		end = MPI_Wtime();
		cout << "Processing time:" << (end - start) << endl;
		cout << "id:" << id << " result_arity: " << T1T2T3.get_arity() << endl;
		cout << "id:" << id << " result_size: " << T1T2T3.get_size() << endl;
		cout << endl;

	}

	MPI_Finalize();

	return 0;

}

