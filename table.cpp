#include "table.h"
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>
#include "atom.h"
using namespace std;

Table::Table(const char* file){
	    ifstream infile;
	    infile.open(file);
	    vector<Atom > content;
	    int arity = 0;
	    int size = 0;

	    string line;

	    bool isfirstline = true; // using it to make sure that we compute arity only with the fist line
	    int n = 100;
	    while(getline(infile,line)&&(n--)>0){
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
		cout<<"size"<<this->size<<endl;
	    infile.close();

}

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
void Table::add_line(Atom& line){
	if(line.get_arity()!=this->arity){
		printf("error:arity error\n");
		return;
	}
	this->content.push_back(line);
	this->size++;
}
Atom* Table::remove_end_line(){
	if(this->size==0){
		printf("error:size==0");
		return 0;
	}

	Atom a=content.back();
	this->content.pop_back();
	this->size--;
	vector<int> v=a.get_all();

	return new Atom(v);

}
void Table::print_head(int num_line)const{
	if(this->size==0){
		printf("empty!\n");
		return;
	}
	printf("the table is:\n");
	int i=0;
	for(vector<Atom>::const_iterator itr=this->content.begin();itr!=content.end();++itr){
		if(i++>=num_line){
			return;
		}
		itr->print_line();
	}


}
// get arity, get by index get size
int Table::get_arity()const{
	return this->arity;
}
int Table::get_size() const{
	return this->size;
}
Atom* Table::get(int index) const{
	if(index>=this->size){
			printf("error:size==0");
			return 0;
		}
	Atom a=this->content[index];

	vector<int> v=a.get_all();

	return new Atom(v);
}
void Table::sort_table(){
	sort(this->content.begin(),this->content.end(),Atom::compare);
}
