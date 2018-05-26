/*
 * distribute.cpp
 *
 *  Created on: May 18, 2018
 *      Author: yiranchen
 */

//Generic Version
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

//function declaration

//calculation distribution hashmap for two tables with a common variable
//name1 and name2 refer to its location in different tables
map<int, int> hashfunction(Table &t, Table &t2, int name1, int name2,
		int num_p);

//sending and receiving for distribution data of ROOT to all the processors
void send(Table& t, int tag, int num_p);
void receive(Table& t, int tag);

//print functions
void print_bug_result(int id, Table& t, bool info);
void print_bug(int id, Table& t, Table& t2);
void distri_print(Table& t, string name_Table, int id, int arity, int num_p);
int print_vector(vector<int>& v, char * buffer);
int print_table(Table& T, char * buffer);

//suppose all the processors receive the same raw data,
//these functions help to remove data
//that are not supposed to be in the processor id
void distribute(Table& t, map<int, int> m, int id, int name);
void distribute_by_copy(Table* t, Table& t_, map<int, int> m, int id, int name);

//sending and receiving functions for communicating the intermediate results
void send_mut(Table& t, int id, int tag, map<int, int> m, int name, int num_p);
void receive_mut(Table& t, int id, int tag, int src);

// sending and receiving function for collecting individual result to TOOT
void send_toRoot(Table& t, int root, int src);
void receive_inRoot(Table& t, int num_p);
void receive_inRoot_single(Table& t, int src);

//distributed joints for two tables

//suppose every processor receives the different data
//A hashmap for distriution should be given to the function
//will calculate automatically hashmap for data distribution
//name is a size-two vector---
//name[i] is the name of column of "hashmap based variable" in the table i
//common_x is the locations of all the common variable
//dict_x is the lexical order of all common variable
// bool collection==true->all result collected in ROOT
Table joint_distributedly(int id, int num_p, vector<Table *> raw_data,
		map<int, int> m_distribut, vector<int> name,
		vector<vector<int> > common_x, int* dict_x, bool collection);

//suppose every processor receives the same data and the following function
//will calculate automatically hashmap for data distribution
//name is a size-two vector---
//name[i] is the name of column of "hashmap based variable" in the table i
//common_x is the locations of all the common variable
//dict_x is the lexical order of all common variable
// bool collection==true->all result collected in ROOT
Table joint_distributedly(int id, int num_p, vector<Table *> raw_data,
		vector<int> name, vector<vector<int> > common_x, int* dict_x,
		bool collection);

// function implementation
void send_all_toRoot(Table& t, int root, int src) {
	int arity = t.get_arity();
	int table_size = t.get_size();
	MPI_Status status;
	int destination = root;
	//send data
	cout << "sending start:" << src << endl;
	MPI_Send(&table_size, 1, MPI_INT, destination, TAG_RESULT + src,
			MPI_COMM_WORLD);
	int *all = new int[arity * table_size];
	int ni = 0;
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		Atom a = *itr;
		vector<int> atomv = a.get_all();

		for (int i = 0; i < atomv.size(); i++) {
			all[ni++] = atomv[i];
		}
	}
	//assert(ni==arity*table_size);
	MPI_Send(all, arity * table_size, MPI_INT, destination, TAG_RESULT + src,
			MPI_COMM_WORLD);

	//return signal that sending finished

	cout << "sending end:" << src << endl;
	delete[] all;
}

void receive_all_inRoot(Table& t, int src) {

	MPI_Status status;
	int arity = t.get_arity();
	int table_size;

	//        vector<int> atomv(arity, 0);
	//cout << "reception start:" << src << endl;
	MPI_Recv(&table_size, 1, MPI_INT, src, TAG_RESULT + src, MPI_COMM_WORLD,
			&status);
	int* all = new int[arity * table_size];

	MPI_Recv(all, arity * table_size, MPI_INT, src, TAG_RESULT + src,
			MPI_COMM_WORLD, &status);

	//write into table
	vector<int> atomv(arity);
	for (int i = 0; i < table_size; i++) {
		for (int j = 0; j < arity; j++) {
			atomv[j] = all[i * arity + j];
		}
		Atom a = Atom(atomv);
		t.add_line(a);
	}
	//cout << "reception end :" << src << endl;
	delete[] all;

}

void send(Table& t, int tag, int num_p) {
	cout << "Number_Pro:" << num_p << endl;
	//construction of hashmap for balanced distribution
	//send data
	int arity = t.get_arity();
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		MPI_Request reqs;
		MPI_Status status;
		Atom atom = *itr;
		vector<int> a = atom.get_all();
		for (int i = 1; i < num_p; i++) {
			MPI_Isend(&a[0], arity, MPI_INT, i, tag, MPI_COMM_WORLD, &reqs);
			MPI_Wait(&reqs, &status);
		}

	}

	//return signal that sending finished
	for (int i = 0; i < num_p; i++) {
		MPI_Request reqs;
		MPI_Status status;
		vector<int> a(arity, -1);

		MPI_Isend(&a[0], arity, MPI_INT, i, tag, MPI_COMM_WORLD, &reqs);
		MPI_Wait(&reqs, &status);

	}
}
void receive(Table& t, int tag) {
	int arity = t.get_arity();
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
void distribute_by_copy(Table* t, Table& t_, map<int, int> m, int id,
		int name) {
	for (vector<Atom>::iterator itr = t_.begin(); itr != t_.end(); itr++) {
		int key = itr->get(name);
		//copy into t for t_ if m(key)==id
		if (m[key] == id) {
			Atom a = *itr;
			t->add_line(a);
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

void send_mut(Table& t, int id, int tag, map<int, int> m, int name, int num_p) {

	//construction of hashmap for balanced distribution
	//send data
	int arity = t.get_arity();
	vector<Atom>::iterator final = t.end();

	cout << "sending start:" << id << endl;
	int *atom = new int[arity];
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {

		Atom a = *itr;
		vector<int> atomv = a.get_all();
		int dest = m[atomv[name]];
		if (dest == id)
			continue;

		for (int i = 0; i < atomv.size(); i++) {
			atom[i] = atomv[i];
		}
		MPI_Send(atom, arity, MPI_INT, dest, dest, MPI_COMM_WORLD);

	}

	//return signal that sending finished
	vector<int> atomv(arity, -1);
	for (int i = 0; i < arity; i++) {
		atom[i] = -1;

	}
	for (int i = 0; i < num_p; i++) {
		if (i == id)
			continue;
		MPI_Send(atom, arity, MPI_INT, i, i, MPI_COMM_WORLD);
	}
	delete atom;

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

void receive_mut(Table& t, int id, int tag, int src) {

	MPI_Status status;
	int arity = t.get_arity();
	int* atom = new int[arity];
	vector<int> atomv(arity);

	for (int i = 0; i < arity; i++) {
		atom[i] = 0;
	}

	while (atom[0] >= 0) {

		MPI_Recv(atom, arity, MPI_INT, src, id, MPI_COMM_WORLD, &status);

		if (atom[0] >= 0) {

			for (int i = 0; i < arity; i++) {
				atomv[i] = atom[i];
			}

			Atom a = Atom(atomv);
			t.add_line(a);
		}

	}

	delete atom;

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

Table joint_distributedly(int id, int num_p, vector<Table *> raw_data,
		map<int, int> m_distribut, vector<int> name,
		vector<vector<int> > common_x, int* dict_x, bool collection) {
	vector<int> all_arity(0);
	vector<Table*> all_Table(0);
	int num_table = 2;
	//examination for input type
	if (raw_data.size() != 2) {
		cout << "error_in_joint_distributed!!! " << "ID:" << id
				<< " raw_data_size_error" << endl;
		return Table(0);
	}
	if (common_x.size() != 2) {
		cout << "error_in_joint_distributed!!! " << "ID:" << id
				<< " common_x_size_error" << endl;
		return Table(0);
	}
	if (name.size() != 2) {
		cout << "error_in_joint_distributed!!! " << "ID:" << id
				<< " name_size_error" << endl;
		return Table(0);
	}

	//construction for all_arity
	for (int i = 0; i < num_table; i++) {
		all_arity.push_back(raw_data[i]->get_arity());
	}

	//construction for data buffer
	for (int i = 0; i < num_table; i++) {
		Table *t = new Table(all_arity[i]);
		all_Table.push_back(t);
	}
	//calculate hashfunction for distribution
	map<int, int> m = m_distribut;
	//	smarter way for distributionßß
	for (int i = 0; i < num_table; i++) {
		distribute_by_copy(all_Table[i], *raw_data[i], m, id, name[i]);
	}

	MPI_Barrier (MPI_COMM_WORLD);
	cout << "distributed_T1 size:" << all_Table[0]->get_size() << " T2 size:"
			<< all_Table[1]->get_size() << endl;
	if (id == ROOT) {
		cout << "distribution finished!" << endl;
	}

	//	 joint raw data in each processors

	if (id == ROOT) {
		cout << "starting joint" << endl;
	}
	//joits

	Table t_result = Join::join(*all_Table[0], *all_Table[1], common_x, dict_x);
	cout << "id:" << id << " result_arity: " << t_result.get_arity() << endl;
	cout << "id:" << id << " result_size: " << t_result.get_size() << endl;

	MPI_Barrier(MPI_COMM_WORLD);
	if (id == ROOT) {
		cout << "joint_finished" << endl;
	}
	//	  resending results of processors to root
	if (collection) {
		for (int i = 1; i < num_p; i++) {
			MPI_Barrier(MPI_COMM_WORLD);
			if (id != ROOT) {
				if (id == i)
					send_all_toRoot(t_result, ROOT, id);
			} else {
				receive_all_inRoot(t_result, i);
			}
		}
	}
	// Print off result
	MPI_Barrier(MPI_COMM_WORLD);
	if (id == ROOT) {
		print_bug_result(ROOT, t_result, false);
	}

	for (int i = 0; i < num_table; i++) {
		delete all_Table[i];
	}

	return t_result;

}
Table joint_distributedly(int id, int num_p, vector<Table *> raw_data,
		vector<int> name, vector<vector<int> > common_x, int* dict_x,
		bool collection) {
	vector<int> all_arity(0);
	vector<Table*> all_Table(0);
	int num_table = 2;
	//examination for input type
	if (raw_data.size() != 2) {
		cout << "error_in_joint_distributed!!! " << "ID:" << id
				<< " raw_data_size_error" << endl;
		return Table(0);
	}
	if (common_x.size() != 2) {
		cout << "error_in_joint_distributed!!! " << "ID:" << id
				<< " common_x_size_error" << endl;
		return Table(0);
	}
	if (name.size() != 2) {
		cout << "error_in_joint_distributed!!! " << "ID:" << id
				<< " name_size_error" << endl;
		return Table(0);
	}

	//construction for all_arity
	for (int i = 0; i < num_table; i++) {
		all_arity.push_back(raw_data[i]->get_arity());
	}

	//construction for data buffer
	for (int i = 0; i < num_table; i++) {
		Table *t = new Table(all_arity[i]);
		all_Table.push_back(t);
	}
	//calculate hashfunction for distribution
	map<int, int> m = hashfunction(*raw_data[0], *raw_data[1], name[0], name[1],
			num_p);
	//	smarter way for distributionßß
	for (int i = 0; i < num_table; i++) {
		distribute_by_copy(all_Table[i], *raw_data[i], m, id, name[i]);
	}

	MPI_Barrier (MPI_COMM_WORLD);
	cout << "distributed_T1 size:" << all_Table[0]->get_size() << " T2 size:"
			<< all_Table[1]->get_size() << endl;
	if (id == ROOT) {
		cout << "distribution finished!" << endl;
	}

	//	 joint raw data in each processors

	if (id == ROOT) {
		cout << "starting joint" << endl;
	}
	//joits

	Table t_result = Join::join(*all_Table[0], *all_Table[1], common_x, dict_x);
	cout << "id:" << id << " result_arity: " << t_result.get_arity() << endl;
	cout << "id:" << id << " result_size: " << t_result.get_size() << endl;

	MPI_Barrier(MPI_COMM_WORLD);
	if (id == ROOT) {
		cout << "joint_finished" << endl;
	}
	//	  resending results of processors to root
	if (collection) {
		for (int i = 1; i < num_p; i++) {
			MPI_Barrier(MPI_COMM_WORLD);
			if (id != ROOT) {
				if (id == i)
					send_all_toRoot(t_result, ROOT, id);
			} else {
				receive_all_inRoot(t_result, i);
			}
		}
	}
	// Print off result
	MPI_Barrier(MPI_COMM_WORLD);
	if (id == ROOT) {
		print_bug_result(ROOT, t_result, false);
	}

	for (int i = 0; i < num_table; i++) {
		delete all_Table[i];
	}

	return t_result;

}

// using var%num_p for data distribution-----Q5
map<int, int> hashfunctionQ5(Table &t, Table &t2, int name1, int name2,
		int num_p) {
	map<int, int> m;
	int id_p = 0;
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		int key = itr->get(name1);
		if (m.find(key) == m.end()) {
			m[key] = key % num_p;
		}
	}
	for (vector<Atom>::iterator itr = t2.begin(); itr != t2.end(); itr++) {
		int key = itr->get(name2);
		if (m.find(key) == m.end()) {
			m[key] = key % num_p;
		}
	}

	return m;
}
int* send_all_to(Table& t, int root, int src, MPI_Request *request) {
	int arity = t.get_arity();
	int table_size = t.get_size();
	MPI_Status status;
	MPI_Request re;
	int destination = root;
	//send data
	cout << "sending start:" << src << endl;
	MPI_Send(&table_size, 1, MPI_INT, destination, TAG_RESULT*2 + src,
			MPI_COMM_WORLD);
	int *all=new int[arity * table_size];
	int ni = 0;
	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {
		Atom a = *itr;
		vector<int> atomv = a.get_all();

		for (int i = 0; i < atomv.size(); i++) {
			all[ni++] = atomv[i];
		}
	}
	//assert(ni==arity*table_size);
	MPI_Isend(all, arity * table_size, MPI_INT, destination, TAG_RESULT + src,
			MPI_COMM_WORLD, request);

	//return signal that sending finished

	cout << "sending end:" << src << endl;
	return all;
}

vector<int *> send_mut2(Table& t, int id, map<int, int> m, int name, int num_p,
		MPI_Request *request) {

	//construction of hashmap for balanced distribution
	//send data
	vector<int *> trash(0);
	int arity = t.get_arity();
	cout << "sending start:" << id << endl;
	vector<Table*> bag(num_p);
	for (int i = 0; i < num_p; i++) {
		bag[i] = new Table(arity);
	}

	for (vector<Atom>::iterator itr = t.begin(); itr != t.end(); itr++) {

		Atom a = *itr;
		vector<int> atomv = a.get_all();
		int dest = m[atomv[name]];
		if (dest == id)
			continue;
		bag[dest]->add_line(a);

	}
	for (int i = 0; i < num_p; i++) {
		if (i == id)
			continue;
		int * temp=send_all_to(*bag[i], i, id, request + i);
		trash.push_back(temp);
	}
	for (int i = 0; i < num_p; i++) {
		delete bag[i];
	}
	return trash;

}

void receive_mut2(Table& t, int src) {
	MPI_Status status;
	int arity = t.get_arity();
	int table_size;

	//        vector<int> atomv(arity, 0);
	//cout << "reception start:" << src << endl;
	MPI_Recv(&table_size, 1, MPI_INT, src, TAG_RESULT*2 + src, MPI_COMM_WORLD,
			&status);
	int* all = new int[arity * table_size];

	MPI_Recv(all, arity * table_size, MPI_INT, src, TAG_RESULT + src,
			MPI_COMM_WORLD, &status);

	//write into table
	vector<int> atomv(arity);
	for (int i = 0; i < table_size; i++) {
		for (int j = 0; j < arity; j++) {
			atomv[j] = all[i * arity + j];
		}
		Atom a = Atom(atomv);
		t.add_line(a);
	}
	//cout << "reception end :" << src << endl;
	delete[] all;

}

vector<int *> communicate(Table& t, int id, map<int, int> m, int name, int num_p) {

	MPI_Request request[num_p];

	// sending
	vector<int *>trash=send_mut2(t, id, m, 0, num_p, request);
	//receiving
	for (int i = 0; i < num_p; i++) {
		if (i == id)
			continue;
		receive_mut2(t, i);
	}

	cout << "sending receiving end:" << id << endl;
	return trash;

}

void testQ7(int argc, char *argv[]) {
	//setting up for MPI environment
	int id, num_p, num_table;
	MPI_Status status;
	MPI_Request reqs;
	// Initialize MPI.
	MPI_Init(&argc, &argv);
	// Get the number of processes.
	MPI_Comm_size(MPI_COMM_WORLD, &num_p);
	// Get the individual process ID.
	MPI_Comm_rank(MPI_COMM_WORLD, &id);

	//setting up parameters for joints

	vector<Table*> result_Table(0);
	vector<int> name(0);
	vector<vector<int> > common_x(0);

	//starting run time
	double start, end;
	if (id == ROOT) {
		start = MPI_Wtime();
	}

	//loading raw Data
	const char* file_list[] = { "twitter.dat", "twitter.dat" };
	vector<Table*> raw_data(0);
	num_table = 2;
	for (int i = 0; i < 2; i++) {
		Table *t = new Table(file_list[i]);
		raw_data.push_back(t);
	}
	//[x1,x2] [x2,x3] [x1,x3]
	name.push_back(1);
	name.push_back(0);

	//with x2 for [x1,x2] [x2,x3]
	map<int, int> m1 = hashfunction(*raw_data[0], *raw_data[1], name[0],
			name[1], num_p);

	// with x1 for [x1,x2,x3] [x3,x1]
	map<int, int> m2 = hashfunction(*raw_data[0], *raw_data[1], 0, 1, num_p);

	//setting up parameters for joints
	{
		vector<int> v1_(1, 1);
		vector<int> v2_(1, 0);
		common_x.push_back(v1_);
		common_x.push_back(v2_);
	}
	int dict_x[] = { 0 };

	// distributed joint for [x1,x2] [x2,x3]
	Table t_result_buffer = joint_distributedly(id, num_p, raw_data, m1, name,
			common_x, dict_x, false);
	{
		cout << "result_buffer:" << t_result_buffer.get_size() << endl;
	}
	//redistribution of intermediate result

	MPI_Request myRequest[num_p];
	MPI_Status myStatus[num_p];

	MPI_Barrier (MPI_COMM_WORLD);
	if (id == ROOT) {
		cout << "mutual communication starts" << endl;
	}

	vector<int *>trash=communicate(t_result_buffer, id, m2, 0, num_p);;

	MPI_Barrier(MPI_COMM_WORLD);
	if (id == ROOT) {
			cout << "mutual communication ends" << endl;
		}

	// cleaning trash
	for(vector<int*>::iterator itr=trash.begin();itr!=trash.end();itr++){
		delete [] *itr;
	}

	cout << "get_itermediate_result_size:" << t_result_buffer.get_size()
			<< " arity:" << t_result_buffer.get_arity() << endl;

	MPI_Barrier(MPI_COMM_WORLD);

	if (id == ROOT)
		cout << "distribution intermediate_result finished" << endl;

	//third joint[x1,x2,x3] [x3,x1]
	raw_data[0] = &t_result_buffer;
	//setting up parameters
	{
		int x1[] = { 0, 2 };
		int x2[] = { 1, 0 };
		vector<int> v1_(x1, x1 + 2);
		vector<int> v2_(x2, x2 + 2);
		common_x[0] = v1_;
		common_x[1] = v2_;
	}
	name[0] = 0;
	name[1] = 1;
	//	map<int,int>m2 = hashfunction(*raw_data[0], *raw_data[1], name[0], name[1], num_p);
	int dict_x2[] = { 0, 1 };
	joint_distributedly(id, num_p, raw_data, m2, name, common_x, dict_x2, true);

	// Terminate MPI.

	if (id == ROOT) {
		end = MPI_Wtime();
		cout << "Processing time:" << (end - start) << endl;
	}

	MPI_Finalize();
}

int main(int argc, char *argv[]) {

	testQ7(argc, argv);
	return 0;

}

