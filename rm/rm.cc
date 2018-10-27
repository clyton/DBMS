
#include "rm.h"

#include "../rbf/pfm.h"

RC success = 0;
enum TableType
{
  SYSTEM_TABLE,
  USER_TABLE
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
  return success;
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
  return success;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  const string fileName = tableName + ".tbl";
  FileHandle fileHandle;
  rbfm->createFile(fileName);

  // insert tuple in table catalog
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
  return tableId;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  int tableId = getTableIdByName(tableName);
  RM_ScanIterator rmsi;
  vector<string> attributeNames;
  for (int i = 1; i < colRecordDescriptor.size() - 1; i++)
  {
    attributeNames.push_back(colRecordDescriptor[i].name);
  }
  scan(tableName, "table-id", EQ_OP, &tableId, attributeNames, rmsi);
  RID ridScan;
  void *attrTuple = malloc(PAGE_SIZE);
  while (rmsi.getNextTuple(ridScan, attrTuple) != RM_EOF)
  {
    Attribute attr;
    unsigned offset = 1; //null indicator for tuple //TODO: Confirm if need to check for null values

    int nameLength = 0;
    memcpy(&nameLength, (char *)attrTuple + offset, 4);
    offset += 4;

    void *name = malloc(50);
    memcpy(name, (char *)attrTuple + offset, nameLength);
    offset += nameLength;

    attr.name = string((char *)name, nameLength);

    int attrType = -1;
    memcpy(&attrType, (char *)attrTuple + offset, 4);
    offset += 4;

    attr.type = (AttrType)attrType;

    int attrLength = -1;
    memcpy(&attrLength, (char *)attrTuple + offset, 4);
    offset += 4;

    attr.length = (AttrLength)attrLength;

    attrs.push_back(attr);
    free(name);
  }
  free(attrTuple);
  return success;
}

bool isSystemTable(const string &tableName)
{
  if (tableName.compare("Tables") == 0 || tableName.compare("Columns"))
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
  if (rbfm->openFile(tableName, fileHandle) != 0)
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
  if (rbfm->openFile(tableName, fileHandle) != 0)
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
  if (rbfm->openFile(tableName, fileHandle) != 0)
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
  return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
  return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
  return -1;
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
