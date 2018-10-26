/*
 * UnitTest.cc
 *
 *  Created on: Oct 19, 2018
 *      Author: dantis
 */

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"
#include <stdlib.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

bool isFieldNullTest(unsigned char *nullIndicatorArray, int fieldIndex)
{
  int byteNumber = fieldIndex / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - fieldIndex % 8));
  return isNull;
}

int UnitTest(RecordBasedFileManager *rbfm)
{
  // Functions tested
  // 1. Create Record-Based File
  // 2. Open Record-Based File
  // 3. Insert Record
  // 4. Read Record
  // 5. Read Attribute
  // 6. Scan Attribute
  // 7. Close Record-based File
  // 8. Destroy Record-Based File
  cout << endl
       << "***** In RBF Test Case 8 *****" << endl;

  RC rc;
  string fileName = "unitTest";

  // Create a file named "test8"
  rc = rbfm->createFile(fileName);
  assert(rc == success && "Creating the file should not fail.");

  rc = createFileShouldSucceed(fileName);
  assert(rc == success && "Creating the file should not fail.");

  // Open the file "test8"
  FileHandle fileHandle;
  rc = rbfm->openFile(fileName, fileHandle);
  assert(rc == success && "Opening the file should not fail.");

  RID rid;
  int recordSize = 0;
  void *record = NULL;
  void *returnedData = malloc(100);

  vector<Attribute> recordDescriptor;
  createRecordDescriptor(recordDescriptor);

  //    // Initialize a NULL field indicator
  //    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
  //    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
  //    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

  // Insert a record into a file and print the record
  //    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
  RawRecordPreparer rrp = RawRecordPreparer(recordDescriptor);
  record = rrp.setField("Anteater")
               //    		.setField(25)
               .setNull()
               .setField(177.8f)
               .setField(6200)
               .prepareRecord();
  cout << endl
       << "Inserting Data:" << endl;
  rbfm->printRecord(recordDescriptor, record);

  rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
  assert(rc == success && "Inserting a record should not fail.");

  // Given the rid, read the record from file
  rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
  assert(rc == success && "Reading a record should not fail.");

  cout << endl
       << "Returned Data:" << endl;
  rbfm->printRecord(recordDescriptor, returnedData);

  // Compare whether the two memory blocks are the same
  if (memcmp(record, returnedData, recordSize) != 0)
  {
    cout << "[FAIL] Test Case 8 Failed!" << endl
         << endl;
    free(record);
    free(returnedData);
    return -1;
  }

  cout << endl;

  // Compare whether the string attribute is the same or not
  char *attributeData = (char *)malloc(10);
  rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "EmpName", attributeData);
  assert(rc == success && "reading attribute should not fail");

  unsigned char nullIndicatorArray = attributeData[0];
  if (!isFieldNullTest(&nullIndicatorArray, 0))
    assert(strcmp(attributeData + 1, "Anteater") == 0 && "Anteater attribute should be same");

  //Test scan attribute
  RBFM_ScanIterator rbfm_scan;
  vector<string> conditionAttributes;
  conditionAttributes.push_back("Salary");
  rc = rbfm->scan(fileHandle, recordDescriptor, "EmpName", EQ_OP, "Anteater", conditionAttributes, rbfm_scan);
  assert(rc == success && "RBFM::scan() should not fail.");

  RID ridScan;
  void *returnedDataScan = malloc(200);
  int salaryReturned = 0;
  while (rbfm_scan.getNextRecord(ridScan, returnedDataScan) != RBFM_EOF)
  {
    cout << "Returned Salary: " << *(int *)((char *)returnedDataScan + 1) << endl;
    salaryReturned = *(int *)((char *)returnedDataScan + 1);
    assert(salaryReturned == 6200 && "getNextRecord() should not fail.");
  }

  // Close the file "test8"
  rc = rbfm->closeFile(fileHandle);
  assert(rc == success && "Closing the file should not fail.");

  // Destroy the file
  rc = rbfm->destroyFile(fileName);
  assert(rc == success && "Destroying the file should not fail.");

  rc = destroyFileShouldSucceed(fileName);
  assert(rc == success && "Destroying the file should not fail.");

  free(record);
  free(returnedData);

  cout << "RBF Test Case 8 Finished! The result will be examined." << endl
       << endl;

  return 0;
}

int main()
{
  // To test the functionality of the record-based file manager
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  RC rcmain = UnitTest(rbfm);
  return rcmain;
}
