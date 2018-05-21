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

void send(Table& t, map<int, int> m, int num_p, int tag, int arity);

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

void send(Table& t, map<int, int> m, int num_p, int tag, int name, int arity) {

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
		vector<vector<int> > common_x, int* dict_x) {
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

	if(id==ROOT){
		cout<<"starting joint"<<endl;
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

	for (int i = 1; i < num_p; i++) {
		MPI_Barrier(MPI_COMM_WORLD);
		if (id != ROOT) {
			if (id == i)
				send_toRoot(t_result, ROOT, id);
		} else {
			receive_inRoot_single(t_result, i);
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

Table joint_distributedly(int id, int num_p, vector<Table *> raw_data, vector<int> name,
		vector<vector<int> > common_x, int* dict_x) {
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
	map<int, int> m = hashfunction(*raw_data[0], *raw_data[1], name[0], name[1], num_p);
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

	if(id==ROOT){
		cout<<"starting joint"<<endl;
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

	for (int i = 1; i < num_p; i++) {
		MPI_Barrier(MPI_COMM_WORLD);
		if (id != ROOT) {
			if (id == i)
				send_toRoot(t_result, ROOT, id);
		} else {
			receive_inRoot_single(t_result, i);
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


int main(int argc, char *argv[]) {
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
	const char* file_list[] = { "facebook.dat.txt", "facebook.dat.txt" };
	vector<Table*> raw_data(0);
	num_table = 2;
	for (int i = 0; i < 2; i++) {
		Table *t = new Table(file_list[i]);
		raw_data.push_back(t);
	}
//[x1,x2] [x2,x3] [x1,x3]
	name.push_back(1);
	name.push_back(0);

//setting up parameters for joints
	{
		vector<int> v1_(1, 1);
		vector<int> v2_(1, 0);
		common_x.push_back(v1_);
		common_x.push_back(v2_);
	}
	int dict_x[] = { 0 };
//	map<int, int> m = hashfunction(*raw_data[0], *raw_data[1], name[0], name[1],
//			num_p);

// distributed joint
	Table t_result_buffer = joint_distributedly(id, num_p, raw_data, name,
			common_x, dict_x);

//distribute intermediate result to all slaves
	Table t_result = Table(t_result_buffer.get_arity());
	if (id != ROOT) {
		receive(t_result, 0);
	} else {
		t_result = t_result_buffer;
		send(t_result, 0, num_p);
	}

	cout<<"get_result_size:"<<t_result.get_size()<<" arity:"<<t_result.get_arity()<<endl;
	MPI_Barrier(MPI_COMM_WORLD);

	if(id==ROOT)cout<<"distribution intermediate_result finished"<<endl;
	//third joint[x1,x2,x3] [x3,x1]
	raw_data[0]=&t_result;
	//setting up parameters
	{
			int x1[] = { 0, 2 };
			int x2[] = { 1, 0 };
			vector<int> v1_(x1, x1 + 2);
			vector<int> v2_(x2, x2 + 2);
			common_x[0] = v1_;
			common_x[1] = v2_;
		}
	name[0]=2;
	name[1]=0;
//	map<int,int>m2 = hashfunction(*raw_data[0], *raw_data[1], name[0], name[1], num_p);
	int dict_x2[] = { 0,1 };
	joint_distributedly(id, num_p, raw_data, name,
				common_x, dict_x2);

// Terminate MPI.

	if (id == ROOT) {
		end = MPI_Wtime();
		cout << "Processing time:" << (end - start) << endl;
	}

	MPI_Finalize();

	return 0;

}

