
#include "rm.h"

#include <stdlib.h>
#include <cstring>
#include <iostream>

#include "../rbf/pfm.h"

RC success = 0;
RC failure = 1;

enum TableType
{
  SYSTEM_TABLE = 0,
  USER_TABLE = 1
};

RelationManager *RelationManager::instance()
{
  static RelationManager _rm;
  return &_rm;
}

RelationManager::RelationManager()
{
  rbfm = RecordBasedFileManager::instance();

  tblRecordDescriptor.push_back((Attribute){"table-type", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-name", TypeVarChar, 50});
  tblRecordDescriptor.push_back((Attribute){"file-name", TypeVarChar, 50});

  // Describe schema for Columns catalog table
  // Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
  colRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-name", TypeVarChar, 50});
  colRecordDescriptor.push_back((Attribute){"column-type", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-length", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-position", TypeInt, 4});

  currentTableIDRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
  FileHandle fileHandle;
  rbfm->createFile(tableCatalog);
  rbfm->openFile(tableCatalog, fileHandle);

  //  Prepare raw table record for insertion
  //	Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  RawRecordPreparer tblCtlgPrp = RawRecordPreparer(tblRecordDescriptor);
  char *tableCatalogRecord = tblCtlgPrp
                                 .setField(SYSTEM_TABLE) //table-type
                                 .setField(1)            //table-id
                                 .setField("Tables")     //table-name
                                 .setField(tableCatalog) //file-name
                                 .prepareRecord();

  // insert first tableCatalog record for 'Tables' table
  RID rid;
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord, rid);
  //  Prepare raw table record for insertion
  //	Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  char *tableCatalogRecord2 = tblCtlgPrp
                                  .setField(SYSTEM_TABLE)  //table-type
                                  .setField(2)             //table-id
                                  .setField("Columns")     //table-name
                                  .setField(columnCatalog) //file-name
                                  .prepareRecord();

  // insert second tableCatalog record for 'Tables' table
  RID rid2;
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord2, rid2);
  rbfm->closeFile(fileHandle);

  // Open file for columns table
  FileHandle fileHandleForCols;
  rbfm->createFile(columnCatalog);
  rbfm->openFile(columnCatalog, fileHandleForCols);

  //  Prepare raw table record for insertion
  //	 (1 , "table-id"        , TypeInt     , 4  , 1)
  //	 (1 , "table-name"      , TypeVarChar , 50 , 2)
  //	 (1 , "file-name"       , TypeVarChar , 50 , 3)
  //	 (2 , "table-id"        , TypeInt     , 4  , 1)
  //	 (2 , "column-name"     , TypeVarChar , 50 , 2)
  //	 (2 , "column-type"     , TypeInt     , 4  , 3)
  //	 (2 , "column-length"   , TypeInt     , 4  , 4)
  //	 (2 , "column-position" , TypeInt     , 4  , 5)

  //	 (1 , "table-id"        , TypeInt     , 4  , 1)
  RawRecordPreparer colCtlgPrp = RawRecordPreparer(colRecordDescriptor);
  char *columnCatalogRecord = colCtlgPrp
                                  .setField(1)          // table-id
                                  .setField("table-id") // column-name
                                  .setField(TypeInt)    // column-type
                                  .setField(4)          // column-length
                                  .setField(1)          // column-position
                                  .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (1 , "table-name"      , TypeVarChar , 50 , 2)
  columnCatalogRecord = colCtlgPrp
                            .setField(1)            // table-id
                            .setField("table-name") // column-name
                            .setField(TypeVarChar)  // column-type
                            .setField(50)           // column-length
                            .setField(2)            // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (1 , "file-name"       , TypeVarChar , 50 , 3)
  columnCatalogRecord = colCtlgPrp
                            .setField(1)           // table-id
                            .setField("file-name") // column-name
                            .setField(TypeVarChar) // column-type
                            .setField(50)          // column-length
                            .setField(3)           // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "table-id"        , TypeInt     , 4  , 1)
  columnCatalogRecord = colCtlgPrp
                            .setField(2)          // table-id
                            .setField("table-id") // column-name
                            .setField(TypeInt)    // column-type
                            .setField(4)          // column-length
                            .setField(1)          // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-name"     , TypeVarChar , 50 , 2)
  columnCatalogRecord = colCtlgPrp
                            .setField(2)             // table-id
                            .setField("column-name") // column-name
                            .setField(TypeVarChar)   // column-type
                            .setField(50)            // column-length
                            .setField(2)             // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-type"     , TypeInt     , 4  , 3)
  columnCatalogRecord = colCtlgPrp
                            .setField(2)             // table-id
                            .setField("column-type") // column-name
                            .setField(TypeInt)       // column-type
                            .setField(4)             // column-length
                            .setField(3)             // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-length"   , TypeInt     , 4  , 4)
  columnCatalogRecord = colCtlgPrp
                            .setField(2)               // table-id
                            .setField("column-length") // column-name
                            .setField(TypeInt)         // column-type
                            .setField(4)               // column-length
                            .setField(4)               // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-position" , TypeInt     , 4  , 5)
  columnCatalogRecord = colCtlgPrp
                            .setField(2)                 // table-id
                            .setField("column-position") // column-name
                            .setField(TypeInt)           // column-type
                            .setField(4)                 // column-length
                            .setField(5)                 // column-position
                            .prepareRecord();
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);
  rbfm->closeFile(fileHandleForCols);

  current_table_id = 3;
  rbfm->createFile(currentTableIDFile);
  FileHandle tableIDFileHandle;
  rbfm->openFile(currentTableIDFile, tableIDFileHandle);
  RID tableIDRID;
  RawRecordPreparer maxIdRecordPrp = RawRecordPreparer(currentTableIDRecordDescriptor);
  char *maxIDRecord = maxIdRecordPrp.setField(current_table_id).prepareRecord();
  rbfm->insertRecord(tableIDFileHandle, currentTableIDRecordDescriptor, maxIDRecord, tableIDRID);
  rbfm->closeFile(tableIDFileHandle);

  return success;
}

void RelationManager::persistCurrentTableId()
{
  FileHandle fileHandle;
  rbfm->openFile(currentTableIDFile, fileHandle);
  RID rid = {0, 0};
  RawRecordPreparer maxIdRecordPrp = RawRecordPreparer(currentTableIDRecordDescriptor);
  char *maxIDRecord = maxIdRecordPrp.setField(current_table_id).prepareRecord();
  rbfm->updateRecord(fileHandle, currentTableIDRecordDescriptor, maxIDRecord, rid);

  rbfm->closeFile(fileHandle);
}

RC RelationManager::deleteCatalog()
{
  if (rbfm->destroyFile(tableCatalog) != 0)
  {
    return -1;
  }

  if (rbfm->destroyFile(columnCatalog) != 0)
  {
    return -1;
  }

  if (rbfm->destroyFile(currentTableIDFile) != 0)
  {
    current_table_id = 1;
    return -1;
  }
  return success;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

  const string fileName = tableName + ".tbl";
  FileHandle fileHandle;
  rbfm->createFile(fileName);

  // insert tuple in table catalog
  readCurrentTableID();
  rbfm->openFile(tableCatalog, fileHandle);
  RawRecordPreparer tblRecordPrp = RawRecordPreparer(tblRecordDescriptor);
  RID rid;
  char *tableCatalogRecord = tblRecordPrp.setField(SYSTEM_TABLE)
                                 .setField(current_table_id)
                                 .setField(tableName)
                                 .setField(fileName)
                                 .prepareRecord();
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord, rid);
  rbfm->closeFile(fileHandle);

  // insert tuples in column catalog
  FileHandle colFileHandle;
  rbfm->openFile(columnCatalog, colFileHandle);
  RawRecordPreparer colRecordPrp = RawRecordPreparer(colRecordDescriptor);
  char *colCatalogRecord;
  int colPosition = 1;
  for (Attribute attr : attrs)
  {
    colCatalogRecord = colRecordPrp
                           .setField(current_table_id)
                           .setField(attr.name)
                           .setField(attr.type)
                           .setField((int)attr.length)
                           .setField(colPosition++)
                           .prepareRecord();
    rbfm->insertRecord(colFileHandle, colRecordDescriptor, colCatalogRecord, rid);
  }

  current_table_id++;
  persistCurrentTableId();

  return success;
}

RC RelationManager::deleteTable(const string &tableName)
{

  return -1;
}

int RelationManager::getTableIdByName(const string &tableName)
{
  RM_ScanIterator rmsi;
  vector<string> tableAttribute;
  tableAttribute.push_back("table-id");
  scan("Tables", "table-name", EQ_OP, &tableName, tableAttribute, rmsi);
  RID rid;
  void *tableIdData = malloc(5);                   // size of int + 1 byte null indicator
  rmsi.getNextTuple(rid, tableIdData);             //output in scan iterator return format
  int tableId = *(int *)((char *)tableIdData + 1); //Assuming table id can not be null ||not checking null indicator
  free(tableIdData);
  return tableId;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  return getRecordDescriptorForTable(tableName, attrs);
}

bool isSystemTable(const string &tableName)
{
  if (tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0)
    return true;
  return false;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  if (isSystemTable(tableName))
    return -1;
  vector<Attribute> attrs;
  if (getAttributes(tableName, attrs) != 0)
    return -1;

  FileHandle fileHandle;
  if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    return -1;

  if (rbfm->insertRecord(fileHandle, attrs, data, rid) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0)
    return -1;

  return success;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  if (isSystemTable(tableName))
    return -1;

  FileHandle fileHandle;
  if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    return -1;

  vector<Attribute> attrs;
  if (getAttributes(tableName, attrs) != 0)
    return -1;

  if (rbfm->deleteRecord(fileHandle, attrs, rid) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0)
    return -1;

  return success;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
  if (isSystemTable(tableName))
    return -1;

  FileHandle fileHandle;
  if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    return -1;

  vector<Attribute> attrs;
  if (getAttributes(tableName, attrs) != 0)
    return -1;

  if (rbfm->updateRecord(fileHandle, attrs, data, rid) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0)
    return -1;

  return success;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
  FileHandle fileHandle;
  if (rbfm->openFile(tableName + ".tbl", fileHandle) != 0)
    return -1;

  vector<Attribute> attrs;
  if (getAttributes(tableName, attrs) != 0)
    return -1;

  if (rbfm->readRecord(fileHandle, attrs, rid, data) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0)
    return -1;
  return success;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  if (rbfm->printRecord(attrs, data) != 0)
    return -1;
  return success;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  FileHandle fileHandle;
  if (rbfm->openFile(tableName, fileHandle) != 0)
    return -1;

  vector<Attribute> attrs;
  if (getAttributes(tableName, attrs) != 0)
    return -1;

  if (rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0)
    return -1;

  return success;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
  rm_ScanIterator.tableName = tableName;
  rm_ScanIterator.conditionAttribute = conditionAttribute;
  rm_ScanIterator.compOp = compOp;
  rm_ScanIterator.value = value;
  rm_ScanIterator.attributeNames = &attributeNames;
  FileHandle *fileHandleLocal = (FileHandle *)malloc(sizeof(FileHandle));
  rbfm->openFile(tableName + ".tbl", *fileHandleLocal);
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);
  //  rbfm->scan(fileHandle, recordDescriptor,
  //		  conditionAttribute, compOp, rbfmScanner value, attributeNames,
  //		  rm_ScanIterator.rbfm_ScanIterator);
  rm_ScanIterator.recordDescriptor = recordDescriptor;
  rm_ScanIterator.fileHandle = fileHandleLocal;
  rbfm->scan(*fileHandleLocal, recordDescriptor,
             conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
  return success;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
  return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
  return -1;
}

RM_ScanIterator::RM_ScanIterator()
{

  rm = RelationManager::instance();
  tableName = "";
  conditionAttribute = "";
  compOp = NO_OP;
  value = NULL;
  attributeNames = NULL;
}

RM_ScanIterator::~RM_ScanIterator()
{
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{

  if (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
  {
    return success;
  }
  else
  {
    return RM_EOF;
  }
}

RC RM_ScanIterator::close()
{
  rbfm->closeFile(*fileHandle);
  free(fileHandle);
  fileHandle = NULL;
  return success;
}

RC RelationManager::getRecordDescriptorForTable(const string tableName, vector<Attribute> &recordDescriptor)
{

  FileHandle fileHandle;
  rbfm->openFile(columnCatalog, fileHandle);
  string conditionAttribute = "table-id";
  CompOp compOp = EQ_OP;
  const int value = getTableIdForTable(tableName);

  if (value == 0)
  {
    rbfm->closeFile(fileHandle);
    return failure;
  }
  vector<string> attributeNames;
  attributeNames.push_back("column-name");
  attributeNames.push_back("column-type");
  attributeNames.push_back("column-length");
  RBFM_ScanIterator rbfm_ScanIterator;
  rbfm->scan(fileHandle, colRecordDescriptor, conditionAttribute,
             compOp, (void *)&value, attributeNames, rbfm_ScanIterator);

  RID rid;
  int isEOF = 0;
  while (isEOF != RBFM_EOF)
  {
    char *data = (char *)malloc(PAGE_SIZE); // max record size
    isEOF = rbfm_ScanIterator.getNextRecord(rid, data);
    Attribute attr;
    // | 1 NIA | 4 bytes varlen| varchar | 4 varlen | varchar | 4 byte int|
    int offset = 1;
    int strlength = 0;
    memcpy(&strlength, data + offset, sizeof(int));
    char *attributeName = (char *)malloc(strlength);
    memcpy(attributeName, data + offset + sizeof(int), strlength);
    offset += sizeof(int) + strlength;

    int attributeType = 0;
    memcpy(&attributeType, data + offset, sizeof(int));
    offset += sizeof(int);

    int attributeLength = 0;
    memcpy(&attributeLength, data + offset, sizeof(int));
    offset += sizeof(int);

    attr.name = attributeName;
    switch (attributeType)
    {
    case TypeInt:
      attr.type = TypeInt;
      break;
    case TypeReal:
      attr.type = TypeReal;
      break;
    case TypeVarChar:
      attr.type = TypeVarChar;
      break;
    }
    attr.length = (AttrLength)attributeLength;

    recordDescriptor.push_back(attr);
    free(data);
    data = NULL;
    free(attributeName);
  }

  rbfm->closeFile(fileHandle);
  return success;
}

int RelationManager::getTableIdForTable(std::string tableName)
{
  FileHandle fileHandle;
  rbfm->openFile(tableCatalog, fileHandle);
  const string &conditionAttribute = "table-name";
  CompOp compOp = EQ_OP;
  char *value = (char *)malloc(4 + tableName.length());
  int valueLength = tableName.length();
  memcpy(value, &valueLength, 4);
  memcpy(value + 4, tableName.c_str(), tableName.length());
  RBFM_ScanIterator rbfm_ScanIterator;
  vector<string> attributeNames;
  attributeNames.push_back("table-id");
  rbfm->scan(fileHandle, tblRecordDescriptor, conditionAttribute, compOp,
             (void *)value, attributeNames, rbfm_ScanIterator);

  int tableid = 0;
  RID rid = {0, 0};
  void *data = malloc(10);
  unsigned char *nullIndicatorArray = (unsigned char *)malloc(1);
  while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
  {
    memcpy(nullIndicatorArray, data, 1);
    if (isFieldNull(nullIndicatorArray, 0))
    {
      cerr << "No such table" << tableName
           << endl;
      break;
    }
    else
    {
      memcpy(&tableid, (char *)data + 1, sizeof(tableid));
      break;
    }
  }
  rbfm->closeFile(fileHandle);
  free(data);
  data = NULL;
  free(value);
  value = NULL;
  return tableid;
}

void RelationManager::readCurrentTableID()
{
  FileHandle fileHandle;
  rbfm->openFile(currentTableIDFile, fileHandle);

  char *data = (char *)malloc(5);
  memset(data, 0, 5);
  rbfm->readAttribute(fileHandle, currentTableIDRecordDescriptor,
                      (RID){0, 0}, "table-id", data);

  // 1 byte for null indicator array
  memcpy(&current_table_id, data + 1, sizeof(current_table_id));
  free(data);
  rbfm->closeFile(fileHandle);
}
