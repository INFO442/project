CC = mpic++

atom.o: atom.cpp atom.h
	$(CC) -c atom.cpp

table.o: table.cpp table.h atom.h
	$(CC) -c table.cpp

join.o: join.cpp join.hpp atom.h table.h
	$(CC) -c join.cpp

quest56.o: quest56.cpp atom.h table.h join.h
	$(CC) -c quest56.cpp

	
Quest56: quest56.o join.o table.o atom.o
	$(CC) atom.o table.o join.o quest56.o -o Quest56

clean: 
	rm -f Quest56
	rm -f *.o
	
