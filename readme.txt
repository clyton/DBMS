1. Basic information
Student ID : 53649044
Student Name : Clyton Dantis
OS (bit) : Linux Ubuntu 18.04
gcc version : 5.4.0


2. Internal Record Format
- Show your record format design and describe how your design satisfies O(1) field access. If not, just mention that you haven't implemented this feature.
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
| Bytes  | 0    1 |      3 |      5 |      7 |      9 |  10 |       14 |        18 |       22 |        26 | 56                        |
| ---->  |        |        |        |        |        |     |          |           |          |           |                           |
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
| Labels | #Cols  | Ptr-F1 | Ptr-F2 | Ptr-F3 | Ptr-F4 | NIA | F1 - Int | F2 - Real | F3 - Int | F4 - VLen | F4 - Actual Var Char Data |
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
| Values | 4      |     14 |     18 |     22 |     26 |   0 |       20 |     100.1 |        2 |        30 | hello world               |
|--------+--------+--------+--------+--------+--------+-----+----------+-----------+----------+-----------+---------------------------|
# LEGENDS
#Cols   : Number of fields/columns in record
Ptr-F1  : Pointer to Field F1
NIA     : Null Indicator Array. Its size is determined by the formula ceil(#cols/8)
F4-Vlen : Length of the Var starting at bit 27


Explanation:
In order to satisfy the O(1) requirement, the above record stores offset value for each field in  bits 3-9.
The offset value stores the start address of a field.
Each pointer occupies 2 bytes. With this organization the nth field's start location can be derived by
startPtr + *(startPtr + n * 2) where 'startPtr' is the starting address of the record obtained from the slot directory.


- Describe how you store a VarChar field.
To store the VarChar field, store the length of the field in 4 bytes followed by the actual string.

3. Page Format
- Show your page format
|---------+---------+---------+----+---------+--------------------------------------------------------------------------------------------------|
| N       | R       | W       | A  |         |                                                                                                  |
| U       | E       | R       | P  |         |                                                                                                  |
| M       | A       | I       | P  |         | <---- MetaData/hidden data                                                                       |
| B       | D       | T       | E  |         |                                                                                                  |
| E       |         | E       | N  |         | This region is initialized in the method createFile() and updated on every closeFile().          |
| R       | C       |         | D  |         | It stores four variables: numberOfPages, readPageCounter, writePageCounter and appendPageCounter |
|         | O       | C       |    |         | Size : 16 bytes.                                                                                 |
| O       | U       | O       | C  |         |                                                                                                  |
| F       | N       | U       | O  |         |                                                                                                  |
| P       | T       | N       | U  |         |                                                                                                  |
| A       | E       | T       | N  |         |                                                                                                  |
| G       | R       | E       | T  |         |                                                                                                  |
| E       |         | R       | E  |         |                                                                                                  |
| S       |         |         | R  |         |                                                                                                  |
|---------+---------+---------+----+---------+--------------------------------------------------------------------------------------------------|
| *******************************************| <----Page 0 : The start of this page is stored in a pointer called PAGE_START. This allows       |
| **********************#####################| for the abstraction of the hidden data. Each page is 4096 bytes long. The records are inserted   |
| ########################################## | starting from the 0th field while the record directory data starts growing from the 4095th field |                                |
|         |         |         |    |         |                                                                                                  |
|         |         |         |    |         |                                                                                                  |
|--------------------------------------------|                                                                                                  |
|         |         |         |    | LENGTH# |                                                                                                  |
|--------------------------------------------|                                                                                                  |
| OFFSET# | LENGTH* | OFFSET* | FS | N       |                                                                                                  |
|---------+---------+---------+----+---------+--------------------------------------------------------------------------------------------------|


The page format features a record directory and slot directories at the end of each page. The record directory structure
consists of two fields FS and N representing the 'Free Space' pointer and 'N'umber of records inserted in the page
respectively. The slot directory structure has two fields: 'Offset' storing the start position of a record and 'Length' which
stores the length of that record. The slot directory structure keeps growing in reverse direction to the records being
inserted in the page i.e The records are inserted from the 0th position and growing to the right of the page while the slot directories
get added to the left end of the page. Each variable in the record directory and slot directory occupies 2 bytes of data. For n records,
the amount of space required for the directories would be 4(n + 1).

OFFSET* refers to start position of the first record denoted by * in Page 0. Similarly
OFFSET# refers to the start position of second record denoted by #. Each record has a slot directory(OFFSET, LENGTH)
that allows O(1) access to the record.

4. Implementation Detail
- Other implementation details goes here.
Page creation and manipulation: A page based file is created using the PageFileManger::createFile method. This method initializes the page stats of the file
at the time of creation. These stats include number of pages, number of page reads, number of page writes and number of page appends. Every time
the file is opened using PageFileManager::openFile(const string &fileName, FileHandle &fileHandle), fileHandle's loadStats method is invoked
which loads these page statistics in the FileStats structure for easy manipulation. When PageFileManager::closeFile method gets invoked the
updated page statistics get written to the hidden page. C's fseek, fwrite, fread functions are used to perform file i/o operations.

Record Insertion: In order to insert a record the following steps are carried out.
i) Find the size required to store the record in the internal format. Use recordDescriptor to find the data type and size of the int and
flot fields. For the varchar fields, read the length of the field from the void memory buffer using memcpy. Parse the fields in the void data
buffer and create a field pointer array. Create the internal record format buffer.
ii) Find the page to store the record. Scan all pages in the file starting from the 0th page and get the first page which has free
space available to store the record. 'Free Space Available = PAGE_SIZE - freeSpacePosition - 4(n+1) - 4' where n is the number of
existing records.
iii) Once the page is found, insert record at freeSpacePointer position using FileHandle::writePage. And update the directory information
    newSlot.offset = freeSpacePtr;
    newSlot.length = recordLength;
    freeSpacePtr += recordLength;
iv) Return RID of the new record. RID (pageNum, newSlot#) of the page where the record was inserted


5. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)
1. Basic information

Team number: 29

#1 Student ID : 53649044 
#1 Student Name : Clyton Dantis 
OS (bit) : Linux Ubuntu 18.04 
gcc version : 5.4.0

#2 Student ID : 61167891 
#2 Student Name : Sandhya Chandramohan 
OS (bit) : MacOS High Sierra 10.13 
gcc version : 5.4.0

2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

* 'Tables' Catalog table schema

| table-type:int | table-id:int | table-name:varchar(50) | file-name:varchar(50) |
|----------------+--------------+------------------------+-----------------------|
| SYSTEM=0       |            1 | Tables                 | Tables.tbl            |
| SYSTEM=0       |            2 | Columns                | Columns.tbl           |
| USER=1         |            3 | Employee               | Employee.tbl          |

The table type column decides whether user has privilege to update those tables.
The file name column stores information about the file where actual records of that table are stored.


* 'Columns' Catalog table design

| table-id:int | column-name:varchar(50) | column-type:int | column-length:int | column-position:int |
|--------------+-------------------------+-----------------+-------------------+---------------------|
|            1 | table-id                | TypeInt         |                 4 |                   1 |
|            1 | table-name              | TypeVarChar     |                50 |                   2 |
|            1 | file-name               | TypeVarChar     |                50 |                   3 |
|            2 | table-id                | TypeInt         |                 4 |                   1 |
|            2 | column-name             | TypeVarChar     |                50 |                   2 |
|            2 | column-type             | TypeInt         |                 4 |                   3 |
|            2 | column-length           | TypeInt         |                 4 |                   4 |
|            2 | column-position         | TypeInt         |                 4 |                   5 |
|            3 | empname                 | TypeVarChar     |                30 |                   1 |
|            3 | age                     | TypeInt         |                 4 |                   2 |
|            3 | height                  | TypeReal        |                 4 |                   3 |
|            3 | salary                  | TypeInt         |                 4 |                   4 |

table-id: This column maps a row in the Columns' table catalog to the table name in Tables catalog
column-name: attribute names of a table
column-type: TypeInt, TypeVarChar and TypeReal are enums representing an int value
column-length: Gives length information about each attribute in a table
column-position : This gives information about the field position of a table attribute


3. Internal Record Format

Design

+--------+-------+------------------+---------------+-------+-------+-------+-------+-------------+----------+-----------+----------+---------------------+--------------------+
| Bytes  | 0   1 |         2        | 3         6   | 7     | 9     | 11    | 13    |      15     | 16       | 20        | 24       | 28                  | 32                 |
+--------+-------+------------------+---------------+-------+-------+-------+-------+-------------+----------+-----------+----------+---------------------+--------------------+
| Labels | #Cols | Tombstone Indtr. | Tombstone RID | Ptr-1 | Ptr-2 | Ptr-3 | Ptr-4 | Null Indtr. | F1 - Int | F2 - Real | F3 - Int | F4 - Varchar Length | F4 - Varchar Value |
+--------+-------+------------------+---------------+-------+-------+-------+-------+-------------+----------+-----------+----------+---------------------+--------------------+
| Values |   4   |         0        |       0       |   16  |   20  |   24  |   28  |      0      |    20    |    12.1   |    23    |          11         | hello world        |
+--------+-------+------------------+---------------+-------+-------+-------+-------+-------------+----------+-----------+----------+---------------------+--------------------+

LEGENDS 
#Cols : Number of fields/columns in record
Tombstone Indtr. : Tombstone Indicator
Tombstone RID : RID of the record when Tombstone Indicator is set to 1
Ptr-F1 : Pointer to Field F1
Null Indtr. : Null Indicator Array. Its size is determined by the formula ceil(#cols/8)
F4- Varchar Length : Length of the Var starting at bit 28

The format is as follows ->
2 bytes of -> No. of columns
1 bytes Tombstone indicator that is set to 1 if the length of the updated record can not be fit in the current page
4 bytes of RID -> (holding the pageNum and slotNum) of the location of the updated record
Null Indicator Array -> 1 byte holding the Null indicator Array -> Corresponding bit of a field is set to 1 if the field is null
Field Pointer Indicator Array -> (2 bytes * Number of Fields) -> Each field of the Field Pointer Array holds the offset location of a field of the record. This results in O(1) field access.

Fields ->
Int Field -> 4 bytes
Real Field -> 4 bytes
Varchar Field -> 4 bytes of length of array followed by the value of the archer field

Update ->
1. If the length of the updated record is less than the length of the old record then, it is written from the offset of the old record. The records following the updated record are shifted to the left. And the slot directory offset of all the records updated and freeSpacePosition updated.
2. If the length of the updated record is more than the length of the old record ->
a. If there is space in the current page to hold the updated record -> The records following the record to be updated are shifted by the diff amount (new record length - old record length) to the right. Following this the record to be updated is updated with the new record. And all the record's slot directories are updated with the offset.
B. If there isn't space in the current page -> The new record data is inserted in the next available page with enough free space and the RID of the position at which this record is inserted is updated in the old records Tombstone RID location and the Tombstone RID of the old record is set to 1. Also the old record is compacted, and all records following it are shifted to the left upto the tombstone RID location.

Delete ->
The RID of the record to be deleted is found. Then, the offset of the record to be deleted is set to USHRT_MAX = 65,535. Following this:
1. If the record to be deleted is the last record in the page -> The free space pointer of the page is updated by the diff amount of length of the record to be deleted and the slot to be deleted offset set to USHRT_MAX
2. If the record to be deleted is not the last record in the page -> The records following the record to be deleted are shifted to the left and their slot directories are updated with the new offsets. The free space pointer of the page is updated by the diff amount of length of the record to be deleted and the slot to be deleted offset set to USHRT_MAX.


4. Page Format

Design

|---------+---------+---------+----+---------+--------------------------------------------------------------------------------------------------|
| N       | R       | W       | A  |         |                                                                                                  |
| U       | E       | R       | P  |         |                                                                                                  |
| M       | A       | I       | P  |         | <---- MetaData/hidden data                                                                       |
| B       | D       | T       | E  |         |                                                                                                  |
| E       |         | E       | N  |         | This region is initialized in the method createFile() and updated on every closeFile().          |
| R       | C       |         | D  |         | It stores four variables: numberOfPages, readPageCounter, writePageCounter and appendPageCounter |
|         | O       | C       |    |         | Size : 16 bytes.                                                                                 |
| O       | U       | O       | C  |         |                                                                                                  |
| F       | N       | U       | O  |         |                                                                                                  |
| P       | T       | N       | U  |         |                                                                                                  |
| A       | E       | T       | N  |         |                                                                                                  |
| G       | R       | E       | T  |         |                                                                                                  |
| E       |         | R       | E  |         |                                                                                                  |
| S       |         |         | R  |         |                                                                                                  |
|---------+---------+---------+----+---------+--------------------------------------------------------------------------------------------------|
| *******************************************| <----Page 0 : The start of this page is stored in a pointer called PAGE_START. This allows       |
| **********************#####################| for the abstraction of the hidden data. Each page is 4096 bytes long. The records are inserted   |
| ########################################## | starting from the 0th field while the record directory data starts growing from the 4095th field |                                |
|         |         |         |    |         |                                                                                                  |
|         |         |         |    |         |                                                                                                  |
|--------------------------------------------|                                                                                                  |
|         |         |         |    | LENGTH# |                                                                                                  |
|--------------------------------------------|                                                                                                  |
| OFFSET# | LENGTH* | OFFSET* | FS | N       |                                                                                                  |
|---------+---------+---------+----+---------+--------------------------------------------------------------------------------------------------|


Update-> If all the records in a page are updated then depending on whether the new records can be fit into the current page or not, the updated records will have tombstone indicators set to 1 and have the tombstones RIDs pointing to the new updated records position. And the records in the page will be compacted or expanded depending on the size of the new record.

Delete-> If all the records in a page are deleted then the slot offset of each of the record in the slot directory will be set to USHRT_MAX = 65535 value. The free space position is incrementally reduced by the length of each deleted record, so finally the free space position will be at 0. The slot directory will still contain all the deleted slots with slot offset = USHRT_MAX. Page will be available for next insert or update of a new record.

5. File Format

Design
+-------------+------------+
| Hidden Page | Pages 0    |
|             |            |
|             |            |
|             |            |
|             |            |
|             |            |
|             |            |
+-------------+------------+
|    Page 0   | 4096 bytes |
|             |            |
|             |            |
|             |            |
|             |            |
|             |            |
|             |            |
+-------------+------------+
|    Page 1   | 4096 bytes |
|             |            |
|             |            |
|             |            |
|             |            |
|             |            |
|             |            |
+-------------+------------+

- Each file contains multiple pages.
- The first page is the Hidden page of 16 bytes which stores the values of the number of pages, number of reads, number of writes, number of pages appended counts.
- Page 0 starts at the end of the hidden page.
- Each page is of 4096 bytes which is the PAGE_SIZE.


6. Implementation Detail
- The USER will not be able to alter the SYSTEM tables -> Tables.tbl and Columns.tbl
- Given the table name, the table descriptor is first found ( Tables.tbl is scanned to get the table-id and Columns.tbl is scanned to find the Columns corresponding to table-id).
Following this a tuple is inserted, updated, deleted or read by calling the corresponding insert record, update record, delete record or read record functions from the recordBasedFileManager
- The scan function, scans each record for the given condition starting from the first page, until a hit is found or the End of File is reached.
1. Basic information
Team number : 29

#1 Student ID : 53649044 
#1 Student Name : Clyton Dantis 
OS (bit) : Linux Ubuntu 18.04 
gcc version : 5.4.0

#2 Student ID : 61167891 
#2 Student Name : Sandhya Chandramohan 
OS (bit) : MacOS High Sierra 10.13 
gcc version : 5.4.0


2. Meta-data page in an index file
+---------------+----------------+------------------+
| ROOT PAGE NUM | Attribute Type | Attribute Length |
|               |                |                  |
|               |                |                  |
|               |                |                  |
|               |                |                  |
|               |                |                  |
|               |                |                  |
+---------------+----------------+------------------+
|    4 bytes    | 4 bytes        |      4 bytes     |
+---------------+----------------+------------------+
|                                                   |
|                   4096 bytes                      |
|                                                   |
+---------------+----------------+------------------+

The following 3 metadata information is stored in a header page in the index file
- The Root page number
- The Attribute Type of the index
- The Attribute Length of the index

The header page is 4096 bytes in size.

3. Index Entry Format

- Non-Leaf Node

+----------+-----+-----+-----------+
| LEFT PTR | KEY | RID | RIGHT PTR |
+----------+-----+-----+-----------+
|          |     |     |           |
+----------+-----+-----+-----------+
|          |     |     |           |
+----------+-----+-----+-----------+

The Non-leaf node entry has:
- A Left Pointer pointing to the PageNum of the index page having keys less than itself
- A Right Pointer pointing to the PageNum of the index page having keys greater than equal to itself
- The Key can be any attribute type - Int, Real or Varchar. Varchar.
- The format of a Varchar key is length (4 bytes) followed by the value of the key
- The Key is followed by the RID of the indexed record which is (PageNum, SlotNum)

- Leaf Node

+---------+---------+
|   KEY   |   RID   |
+---------+---------+
|         |         |
+---------+---------+
|         |         |
+---------+---------+

The Leaf node entry has:
- A Key which can be of any attribute type - Int, Real, Varchar
- The Key is followed by the RID of the indexed record which is (PageNum, SlotNum)

4. Page Format

+-----------+--------------+---------+----------+---------+
| PAGE TYPE | NEIGHBOR PTR |   L/I   |          |         |
+-----------+--------------+---------+----------+---------+
|           |              |         |          |         |
+-----------+--------------+---------+----------+---------+
|           |              |         |          |         |
+-----------+--------------+---------+----------+---------+
|           |              |         |          | OFFSET* |
+-----------+--------------+---------+----------+---------+
|  LENGTH*  |    OFFSET*   | LENGTH* |    FS    |    N    |
+-----------+--------------+---------+----------+---------+
    <---------------------------------
     Sorted Order of Slots

- At the beginning of the page the Page Type is stored which is an enum of 4 bytes
having value LEAF = 0 for a Leaf page and INTERMEDIATE = 1 for a Non-Leaf page
- The NEIGHBOR PTR in a Leaf Page points to the next Leaf page. It is not valid for a Non-Leaf page.
- The NEIGHBOR PTR has value UINT_MAX when in a Non-Leaf Page or in the last Leaf page before EOF
- This is followed by the Leaf or Non-Leaf nodes in the design/format shared above
- N - The no. of slots in the page
- FS - The beginning of the Free Space in the page
- The Slot Directory consisting of the LENGTH and OFFSET of each Leaf/Non-Leaf Intermediate entry
- The Slots in the Slot Directory are in sorted order


5. Implementation Detail
- Have you added your own source file (.cc or .h)?
No have not added any new .cc or .h files.

- Have you implemented non-lazy deletion? Choose Yes or No:
No

- Have you implemented duplicated key handling that can span multiple pages? Choose Yes or No:
Yes.
The Key and RID of the indexed record are together used to uniquely identify a key.
A Custom Comparator function compares on the basis of both Key and then on the basis of RID if Key is same to uniquely determine an entry
The Leafs are also compared using the Custom Comparator function and then inserted in order in the Leaf page

- Other implementation details:
The BTPage class abstracts operations on the Index page.
The Entry class abstracts operations on the Entry. The Leaf and Non-Leaf entries inherit from the Entry class.
The Key class abstracts operations on the Key. The 3 types of Keys, IntKey, StringKey and RealKey inherit from the Key class.
For Insert we traverse to the Leaf node, keeping track of the intermediate nodes traversed in a stack in order to insert by splitting nodes in case there isn't enough space in the page. For searching keys in intermediate and leaf page, we use binary search. So at each node, binary search helps us find the right page to open
The nodes in a page are inserted in sorted order by uniquely comparing them using both Key and RID using the Custom Comparator function
For Delete, lazy deletion is implemented, where we traverse to the entry to be deleted, delete the entry and compact the slot directory and update the free space position and no. of slots in a page
Scan, supports range queries between lowKey and highKey.
1. Basic information
Team number : 29

#1 Student ID : 53649044 
#1 Student Name : Clyton Dantis 
OS (bit) : Linux Ubuntu 18.04 
gcc version : 5.4.0

#2 Student ID : 61167891 
#2 Student Name : Sandhya Chandramohan 
OS (bit) : MacOS High Sierra 10.13 
gcc version : 5.4.0


2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

* 'Tables' Catalog table schema

| table-type:int | table-id:int | table-name:varchar(50) | file-name:varchar(50) |
|----------------+--------------+------------------------+-----------------------|
| SYSTEM=0       |            1 | Tables                 | Tables.tbl            |
| SYSTEM=0       |            2 | Columns                | Columns.tbl           |
| SYSTEM=0       |            3 | Index                  | Index.tbl             |
| USER=1         |            4 | Employee               | Employee.tbl          |

The table type column decides whether user has privilege to update those tables.
The file name column stores information about the file where actual records of that table are stored.


* 'Columns' Catalog table design

| table-id:int | column-name:varchar(50) | column-type:int | column-length:int | column-position:int |
|--------------+-------------------------+-----------------+-------------------+---------------------|
|            1 | table-id                | TypeInt         |                 4 |                   1 |
|            1 | table-name              | TypeVarChar     |                50 |                   2 |
|            1 | file-name               | TypeVarChar     |                50 |                   3 |
|            2 | table-id                | TypeInt         |                 4 |                   1 |
|            2 | column-name             | TypeVarChar     |                50 |                   2 |
|            2 | column-type             | TypeInt         |                 4 |                   3 |
|            2 | column-length           | TypeInt         |                 4 |                   4 |
|            2 | column-position         | TypeInt         |                 4 |                   5 |
|            3 | table-id                | TypeInt         |                 4 |                   1 |
|            3 | index-name              | TypeVarChar     |                50 |                   2 |
|            3 | index-file-name         | TypeVarChar     |                50 |                   3 |
|            4 | empname                 | TypeVarChar     |                30 |                   1 |
|            4 | age                     | TypeInt         |                 4 |                   2 |
|            4 | height                  | TypeReal        |                 4 |                   3 |
|            4 | salary                  | TypeInt         |                 4 |                   4 |

table-id: This column maps a row in the Columns' table catalog to the table name in Tables catalog
column-name: attribute names of a table
column-type: TypeInt, TypeVarChar and TypeReal are enums representing an int value
column-length: Gives length information about each attribute in a table
column-position : This gives information about the field position of a table attribute

* 'Index' Catalog table schema

| table-id:int | index-name:varchar(50) | index-file-name:varchar(50) |
+--------------+------------------------+-----------------------------|
|            5 | B                      | left2_B_idx                 |
|            6 | C                      | right_C_idx                 |
|            7 | B                      | right_B_idx                 |
|            8 | B                      | largeleft2_B_idx            |


- When the catalog is created, a file is created for the index catalog Index.tbl
- table-id: This column maps a row in the Index table catalog to the table name in the Tables catalog
- index-name: This is the same as the attribute name over which the index is created
-index-file-name: This is of the format tableName + _ + indexName + _ + idx. This refers to the name of the index file created for the corresponding index


3. Block Nested Loop Join (If you have implemented this feature)
- Describe how your block nested loop join works (especially, how you manage the given buffers.)
* steps
 Keep reading records from left table until we have read numPages and form in-memory hash table. Then read one record at a time from right table and probe the table. Store matching records in a queue and return the top record. Once all records in queue have been joined and returned, read next right tuple and repeat


4. Index Nested Loop Join (If you have implemented this feature)
- For each record in left table, pick the join attribute and search for index in right table. Then return the matching rows and maintain state for next call


5. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).
Not implemented

6. Aggregation
- Describe how your aggregation (basic, group-based hash) works.
Aggregation (Basic) ->
- The scan iterator, the attibute over which we are performing the aggregation and the aggregate operator are stored as data members in the Aggregate class
- The iterator is used to call getNextTuple to read the tuple and get the value of the attribute over which we are performing the aggregation operation
- The iteration is performed till the end of file
- All aggregates (MAX, MIN, SUM, COUNT, AVERAGE) are calculated as float variables
- They are then returned in the output format, consisting of 1 byte of Null Indicator, followed by 4 bytes of float data

Aggregation (Group-based) -> Described below

7. Implementation Detail
- Have you added your own source file (.cc or .h)?
No

- Have you implemented any optional features? Then, describe them here.
Yes.
Aggregation (Group based hash) ->
- The scan iterator, the attribute over which we are performing the aggregation, the attribute over which we are grouping the aggregation and the aggregate operators are stored as data members in the Aggregate class
- The grouped aggregates are stored as a map in the Aggregate class, with the key being the value of the groupBy attribute as a string and the value being the aggregate operations value for that group
- The getNextTuple returns the next pair from the map in the format of the output (1 byte null indicator + (length of string + string value if varchar) value)

- Other implementation details:
- When an index is created on an attribute in a table-> the corresponding entry is created in the index catalog and all existing tuples in the table on which index  is created are indexed
- When an index is destroyed -> all existing indexes are deleted on that attribute and the corresponding entry is deleted from the index catalog table
- When a tuple is inserted/deleted in a table-> We first check if there exists an index on any attribute in the table by scanning the index catalog -> then we insert/delete the tuple and index the tuple on any attribute if required/ delete the index
