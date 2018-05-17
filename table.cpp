#include "table.h"
#include <iostream>
#include <vector>
#include "atom.h"
using namespace std;

Table::Table(int arity){
	this->arity=arity;
	this->content=vector<Atom>();
	this->size=0;
}
Table::Table(int arity,int size){
	this->arity=arity;
	this->content=vector<Atom>();
	this->size=0;
}
Table::Table(const char* file){
	    ifstream infile;
	    infile.open(file);
	    vector<Atom > content;
	    int arity = 0;
	    int size = 0;

	    string line;

	    bool isfirstline = true; // using it to make sure that we compute arity only with the fist line

	    while(getline(infile,line)){
	        vector<int> tmp;
	        stringstream ss;
	        int localarity =0; // the number of entries in this line
	        ss<<line;
	        int entry;
	        while(ss>> entry){
	        tmp.push_back(entry);
	        localarity++;
	        }

	        if(isfirstline){
	        	arity = localarity;
	        	isfirstline = false;
	        }
	        else{
	        	if(localarity!=arity){
	        		cout<<"Error format of Data: this data has different lines with different arities!!"<<endl;
	        		break;
	        	}
	        }
	        Atom atom(tmp);
	        content.push_back(atom);
	        size++;
	    }
		this->content= content;
		this->arity = arity;
		this->size=size;
	    infile.close();

}

void Table::add_line(Atom& line){
	this->content.push_back(line);
	this->size++;
}
Atom Table::remove_end_line(){
	if(this->size==0){
		printf("error:size==0");
		vector<int> v;


		Atom a(v);
		return a;
	}

	Atom a=content.back();
	this->content.pop_back();
	this->size--;
	return a;
}
void Table::print_head()const{
	if(this->size==0){
		printf("empty!\n");
		return;
	}
	printf("the table is:\n");
	for(vector<Atom>::const_iterator itr=this->content.begin();itr!=content.end();++itr){

		itr->print_line();
	}
}

