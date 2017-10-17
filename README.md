#Tiny Database
We finished the SQL parsing by Flex generator, which is defined by "sanner.l" and "lex.yy.cpp"

We created a class Operations to define functions about those operations, in "Opeartions.h". 

And then we defined several .cpp files for those operations respectively, like "Create.cpp","Select.cpp",etc.

The where clauses are parsed by the Shunting-yard algorithm, which is used to parse infix expressions to generate Reverse Polish Notation (RPN). 

In our implementation, we generated an expression tree rather than the RPN.

#Compile
Before executing the project, you need to compile first. 

Just using make to compile the whole project. You can see the details in "Makefile"
`$ make`

#test
To use the database with an SQL query txt file
`$ ./input TinySQL_linux.txt`
`$ ./input test.txt`
or
`$ ./input < TinySQL_linux.txt`

If you want to input the statement line by line in an interactive SQL environment
just use "./input" to start at first
`$ ./input`

#result 
you can see the result in the terminal immediately. 
And also we output the data into the "TinySQL_output.txt"
Every time when you execute the project, 
those data preserved in the file before would be cleared and replaced by the new data.
