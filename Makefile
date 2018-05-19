#error This file requires compiler and library support for the ISO C++ 2011 standard. This support is currently experimental, and must be enabled with the -std=c++11 or -std=gnu++11 compiler options.

CC = mpic++

atom.o: atom.cpp atom.h
	$(CC) -c atom.cpp $(LIBS)

table.o: table.cpp table.h atom.h
	$(CC) -c table.cpp $(LIBS)

join.o: join.cpp join.hpp atom.h table.h
	$(CC) -c join.cpp $(LIBS)

quest56.o: quest56.cpp atom.h table.h join.h
	$(CC) -c quest56.cpp $(LIBS)

	
Quest56: quest56.o join.o table.o atom.o
	$(CC) atom.o table.o join.o quest56.o -o Quest56 $(LIBS)

clean: 
	rm -f Quest56
	rm -f *.o
	
