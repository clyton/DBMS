# CS 222P Project 2

## 1. Basic information
Team number: 29 

#1 Student ID : 61167891 \
#1 Student Name : Sandhya Chandramohan \
OS (bit) : MacOS High Sierra 10.13  \
gcc version : 5.4.0 

#2 Student ID : 53649044 \
#2 Student Name : Clyton Dantis \
OS (bit) : Linux Ubuntu 18.04 \
gcc version : 5.4.0


## 2. Meta-data

| Tables        | Are           | Cool  | 
| ------------- |:-------------:| -----:|
| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |




## 3. Internal Record Format
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
| Bytes  | 0    1 |      3 |      5 |      7 |      9 |  10 |       14 |        18 |       22 |        26 | 56                        |
| ---->  |        |        |        |        |        |     |          |           |          |           |                           |
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
| Labels | #Cols  | Ptr-F1 | Ptr-F2 | Ptr-F3 | Ptr-F4 | NIA | F1 - Int | F2 - Real | F3 - Int | F4 - VLen | F4 - Actual Var Char Data |
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
| Values | 4      |     14 |     18 |     22 |     26 |   0 |       20 |     100.1 |        2 |        30 | hello world               |
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|

LEGENDS \
#Cols   : Number of fields/columns in record\
Ptr-F1  : Pointer to Field F1\
NIA     : Null Indicator Array. Its size is determined by the formula ceil(#cols/8)\
F4-Vlen : Length of the Var starting at bit 27



- Show your record format design and describe how your design satisfies O(1) field access. 
- Describe how you store a VarChar field.
- Describe how you deal with an update and delete.


## 4. Page Format
- Show your page format design.
- Describe how you deal with an update and delete.


## 5. File Format
- Show your file format design



## 6. Implementation Detail
- Other implementation details goes here.


## 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 2, but not related to the other sections (optional)

