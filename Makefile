CC=g++
LEX=flex
target=input
SM=StorageManager-c++-2_1_beta_1_fix-linux
lib=-lfl 
inc=-I./$(SM)
intermediate=lex.yy.cpp *.o 


all: input
input:  main.o lex.yy.cpp StorageManager.o Operations.o Algorithms.o Create.o common.o Delete.o Drop.o Insert.o Join.o Select.o *.h $(SM)/*.h
	$(CC) $(inc) -g -o $@ main.o StorageManager.o  Operations.o Algorithms.o Create.o  common.o Delete.o Drop.o Insert.o Join.o Select.o $(lib) 
lex.yy.cpp: scanner.l 
	$(LEX) -o $@ $< 
main.o: main.cpp common.h lex.yy.cpp  *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
common.o: common.cpp *.h $(SM)/*.h
	$(CC) $(inc) -g -c -o $@ $<
Algorithms.o: Algorithms.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
Create.o: Create.cpp *.h $(SM)/*.h
	$(CC) $(inc) -g -c -o $@ $<
Operations.o: Operations.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
Delete.o: Delete.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
Drop.o: Drop.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
Insert.o: Insert.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
Join.o: Join.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
Select.o: Select.cpp *.h $(SM)/*.h 
	$(CC) $(inc) -g -c -o $@ $<
StorageManager.o: $(SM)/StorageManager.cpp $(SM)/*.h
	$(CC) -g -c -o $@ $<
clean:
	rm $(target) $(intermediate)
