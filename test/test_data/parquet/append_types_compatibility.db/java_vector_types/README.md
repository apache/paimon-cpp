Schema:

id INT NOT NULL
f_vector_boolean VECTOR<BOOLEAN, 3>
f_vector_tinyint VECTOR<TINYINT, 3>
f_vector_smallint VECTOR<SMALLINT, 3>
f_vector_int VECTOR<INT, 3>
f_vector_bigint VECTOR<BIGINT, 3>
f_vector_float VECTOR<FLOAT, 3>
f_vector_double VECTOR<DOUBLE, 3>

Options: bucket = -1, file.format = parquet. The table is unpartitioned and has no primary
key.

Add: (1, [true,false,true], [-1,0,1], [-1000,0,1000], [-100000,0,100000], [-10000000000,0,10000000000], [1.25,-2.5,3.75], [1.125,-2.25,3.5])
Add: (2, [false,true,false], [2,3,4], [2000,3000,4000], [200000,300000,400000], [20000000000,30000000000,40000000000], [4.25,5.5,6.75], [4.125,5.25,6.5])
