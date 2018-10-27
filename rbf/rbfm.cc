#include "rbfm.h"
#include <type_traits>
#include <stddef.h>
#include <stdlib.h>
#include <bitset>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <cstdlib>

#define SLOT_SIZE sizeof(struct SlotDirectory)
const RC success = 0;
const RC failure = 1;
struct SlotDirectory
{
  r_slot offset = 0;
  r_slot length = 0;
};

struct PageRecordInfo
{
  r_slot numberOfSlots = 0;
  /**
	 * @{code freeSpacePointer} points to the first free space position available in the page
	 *
	 */
  r_slot freeSpacePos = 0;
};

/**
 * Start location of this struct in a page
 */
const r_slot PAGE_RECORD_INFO_OFFSET = PAGE_SIZE - sizeof(struct PageRecordInfo);

void makeFieldNull(unsigned char *nullIndicatorArray, unsigned int fieldIndex)
{
  int byteNumber = fieldIndex / 8;
  nullIndicatorArray[byteNumber] = nullIndicatorArray[byteNumber] | (1 << (7 - fieldIndex % 8));
}

bool isFieldNull(unsigned char *nullIndicatorArray, int fieldIndex)
{
  int byteNumber = fieldIndex / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - fieldIndex % 8));
  return isNull;
}

// char *transformRawToRecordFormat(const void *data, const r_slot numberOfFields, const char isTombstone, const r_slot *fieldPointers, const r_slot recordSizeInBytes)
// {
//   char *record = (char *)malloc(recordSizeInBytes);
//   // Copy no. of fields
//   int recordOffset = 0;
//   memcpy(record, &numberOfFields, sizeof(numberOfFields));
//   recordOffset += sizeof(numberOfFields);

//   // Copy tombstone indicator
//   memcpy(record + recordOffset, &isTombstone, sizeof(isTombstone));
//   recordOffset += sizeof(isTombstone);

//   //adding tombstone indicator pointer size to offset
//   recordOffset += sizeof(RID);

//   // Copy field pointers
//   memcpy(record + recordOffset, fieldPointers,
//          sizeof(fieldPointers[0]) * numberOfFields);
//   recordOffset = recordOffset + sizeof(fieldPointers[0]) * numberOfFields;

//   int rawDataSize = recordSizeInBytes - sizeof(r_slot) - sizeof(char) - sizeof(RID) - (numberOfFields * sizeof(fieldPointers[0]));

//   // Copy Null indicator array + data. Null indicator array already given in data
//   memcpy(record + recordOffset, data, rawDataSize);
//   return record;
// }

r_slot getLengthOfRecordAndTransformRecord(const void *data,
                                           const vector<Attribute> &recordDescriptor, char *record)
{
  r_slot recordSizeInBytes = 0;
  r_slot numberOfFields = recordDescriptor.size();
  r_slot *fieldPointers = new r_slot[numberOfFields];

  // Add 2 byte of space to store number of fields
  recordSizeInBytes += SLOT_SIZE;

  /**
	 * Add the space needed for 1 byte char to express if the record is a tombstone or not
	 * 0 - no tombstone. Actual Data
	 * 1 - tombstone
   * Add 4 bytes of RID struct to hold the tombstone indicator
	 */
  char isTombstone = 0;
  recordSizeInBytes += sizeof(char) + sizeof(RID);

  // Add space needed for field pointer array. Field pointers point to start of field
  recordSizeInBytes = recordSizeInBytes + numberOfFields * sizeof(fieldPointers[0]); // 2 byte pointers for each field
  // cout << "Size of field pointer array is " << numberOfFields * sizeof(fieldPointers[0]) << endl;

  // Add space needed for null bytes indicator
  int nullFieldsIndicatorLength = ceil(numberOfFields / 8.0);
  recordSizeInBytes += nullFieldsIndicatorLength;

  r_slot dataOffset = 0;
  unsigned char *nullIndicatorArray = (unsigned char *)malloc(
      nullFieldsIndicatorLength);
  memcpy(nullIndicatorArray, (char *)data + dataOffset,
         nullFieldsIndicatorLength);
  dataOffset += nullFieldsIndicatorLength;

  // find the memory needed to store a record
  for (size_t attri = 0; attri < recordDescriptor.size(); attri++)
  {
    bool isNull = isFieldNull(nullIndicatorArray, attri);
    if (isNull)
    {
      // cout << "Attribute " << attri << " ISNULL: data at offset " << dataOffset << " is null" << endl;
      // cout << "Attribute " << attri << " ISNULL: record offset : " << recordSizeInBytes << endl;
      // cout << "Attribute " << attri << " ISNULL: data offset : " << dataOffset << endl;
      // cout << "Test bits " << nullattbit << endl;
      // cout << "Test" << nullIndicatorArray[attri] << endl;
      fieldPointers[attri] = USHRT_MAX;
      continue;
    }
    Attribute recordAttribute = recordDescriptor.at(attri);
    if (recordAttribute.type == TypeInt || recordAttribute.type == TypeReal)
    {
      // cout << "Attribute " << attri << " ISNUMBER: record offset : " << recordSizeInBytes << endl;
      // cout << "Attribute " << attri << " ISNUMBER: data offset : " << dataOffset << endl;
      fieldPointers[attri] = recordSizeInBytes; //point to field start
      recordSizeInBytes += 4;
      dataOffset += 4;
    }
    else
    {
      // cout << "Attribute " << attri << " ISVARCHAR: record offset : " << recordSizeInBytes << endl;
      // cout << "Attribute " << attri << " ISVARCHAR: data offset : " << dataOffset << endl;
      fieldPointers[attri] = recordSizeInBytes; // point to field start
      int varStringLength = 0;
      memcpy(&varStringLength, (char *)data + dataOffset, sizeof(int));
      // cout << "Attribute " << attri << " ISVARCHAR: var string length: " << varStringLength << endl;
      recordSizeInBytes = recordSizeInBytes + sizeof(int) + varStringLength;
      dataOffset = dataOffset + sizeof(int) + varStringLength;
    }
  }
  //*record = transformRawToRecordFormat(data, numberOfFields, isTombstone, fieldPointers, recordSizeInBytes);
  //*record = (char *)malloc(recordSizeInBytes);

  // Copy no. of fields
  int recordOffset = 0;
  memcpy(record, &numberOfFields, sizeof(numberOfFields));
  recordOffset += sizeof(numberOfFields);

  // Copy tombstone indicator
  memcpy(record + recordOffset, &isTombstone, sizeof(isTombstone));
  recordOffset += sizeof(isTombstone);

  //adding tombstone indicator pointer size to offset
  recordOffset += sizeof(RID);

  // Copy field pointers
  memcpy(record + recordOffset, fieldPointers,
         sizeof(fieldPointers[0]) * numberOfFields);
  recordOffset = recordOffset + sizeof(fieldPointers[0]) * numberOfFields;

  int rawDataSize = recordSizeInBytes - sizeof(r_slot) - sizeof(char) - sizeof(RID) - (numberOfFields * sizeof(fieldPointers[0]));

  // Copy Null indicator array + data. Null indicator array already given in data
  memcpy(record + recordOffset, data, rawDataSize);

  return recordSizeInBytes;
}

/**
 *
 * @param pri
 * @return the size taken by array of SlotDirectories and the
 * 		   PageRecordInfo structure
 */
r_slot getRecordDirectorySize(const PageRecordInfo &pri)
{
  r_slot numberOfRecords = pri.numberOfSlots;
  // cout << "Entire Record Directory size is calculated as"<< numberOfRecords * sizeof(struct SlotDirectory) + sizeof(struct PageRecordInfo)<< endl;
  return (numberOfRecords * sizeof(struct SlotDirectory) + sizeof(struct PageRecordInfo));
}

void getPageRecordInfo(PageRecordInfo &pageRecordInfo, char const *pageData)
{
  memcpy(&pageRecordInfo, (pageData) + PAGE_RECORD_INFO_OFFSET,
         sizeof(struct PageRecordInfo));
  // cout << "Page Record Info is getting read from"<< 0 + PAGE_RECORD_INFO_OFFSET<< endl;
}

void putPageRecordInfo(PageRecordInfo &pri, char *pageData)
{
  memcpy((pageData) + PAGE_RECORD_INFO_OFFSET, &pri,
         sizeof(struct PageRecordInfo));
  // cout << "Page Record Info is getting added at"<< 0 + PAGE_RECORD_INFO_OFFSET<< endl;
}

void addSlotDirectory(PageRecordInfo &pri, SlotDirectory &slot,
                      char *pageData)
{
  memcpy(
      (pageData) - sizeof(struct SlotDirectory) + PAGE_SIZE - getRecordDirectorySize(pri), &slot,
      sizeof(struct SlotDirectory));
  // cout << "Slot directory is getting added at"<< 0 - sizeof(struct SlotDirectory) + PAGE_SIZE - getRecordDirectorySize(pri) << endl;
}

SlotDirectory getSlotForRID(char const *pageData, RID rid,
                            SlotDirectory &slot)
{
  unsigned int slotNumber = rid.slotNum;

  r_slot slotStartPos = PAGE_SIZE - sizeof(struct PageRecordInfo) - sizeof(struct SlotDirectory) * (slotNumber + 1);
  memcpy(&slot, pageData + slotStartPos, sizeof(struct SlotDirectory));
  // cout << "Slot for RID slot: " << rid.slotNum << " is : "<< 0 + slotStartPos << endl;
  return slot;
}
/**
 *
 * @param rid : RID of the slot to update
 * @param pageData : memory buffer of the page where the slot is present
 * @param updatedSlot : the new slot data
 * @return
 */
RC updateSlotDirectory(const RID &rid, void *pageData,
                       SlotDirectory &updatedSlot)
{
  memcpy(
      (char *)pageData + PAGE_SIZE - sizeof(struct PageRecordInfo) - sizeof(struct SlotDirectory) * (rid.slotNum + 1),
      &updatedSlot, sizeof(updatedSlot));
  return success;
}

RC updatePageRecordInfo(PageRecordInfo &pri, void *pageData)
{
  memcpy((char *)pageData + PAGE_SIZE - sizeof(struct PageRecordInfo), &pri,
         sizeof(struct PageRecordInfo));
  return success;
}
/**
 * Shifts the record by 'byBytesToShift' and updates their offsets
 *
 * @param pageData : the memory buffer on which the record resides
 * @param slotNum : The record slot number
 * @param byBytesToShift : If negative, shift record to left, else shift to
 * right by 'ByBytesToShift'
 * @return
 */
RC shiftRecord(char *pageData, const RID &rid, int byBytesToShift)
{
  SlotDirectory slotToShift;
  getSlotForRID(pageData, rid, slotToShift);

  if (slotToShift.offset + byBytesToShift < 0 ||
      slotToShift.offset + byBytesToShift + slotToShift.length > PAGE_SIZE)
  {
    return failure;
  }

  memmove(pageData + slotToShift.offset + byBytesToShift,
          pageData + slotToShift.offset, slotToShift.length);

  slotToShift.offset += byBytesToShift;

  updateSlotDirectory(rid, pageData, slotToShift);

  return success;
}

/**
 * This method gets the best first page found where a record of size specified
 * by @{code sizeInBytes} will fit.
 * If none of the pages in the file can occupy
 * the record, a new page is appended to the file and its recordDirectory is
 * updated for insert
 *
 * @param fileHandle
 * @param sizeInBytes
 * @param pageData : pointer pointing to a memory buffer of size PAGE_SIZE
 * @return
 */
RID getPageForRecordOfSize(FileHandle &fileHandle, r_slot sizeInBytes,
                           char *pageData)
{
  //	if (sizeInBytes > (PAGE_SIZE - sizeof(struct SlotDirectory)- sizeof(struct PageRecordInfo))) {
  //		return UINT_MAX; // sentinel value to indicate impossible to add record
  //	}
  PageNum pageNum;
  for (pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++)
  {
    fileHandle.readPage(pageNum, pageData);
    PageRecordInfo pageRecordInfo;
    getPageRecordInfo(pageRecordInfo, pageData);
    r_slot freeSpaceAvailable = PAGE_SIZE - getRecordDirectorySize(pageRecordInfo) //slots + pageRecordInfo
                                - (pageRecordInfo.freeSpacePos);                   // pg occupied from top
    if (freeSpaceAvailable > sizeInBytes)
    {
      // Check if a slot position is empty
      RID eachRID;
      eachRID.pageNum = pageNum;
      eachRID.slotNum = 0;
      for (; eachRID.slotNum < pageRecordInfo.numberOfSlots;
           eachRID.slotNum++)
      {
        SlotDirectory currentSlot;
        getSlotForRID(pageData, eachRID, currentSlot);
        if (currentSlot.offset == USHRT_MAX)
          return eachRID;
      }

      SlotDirectory newSlot;
      newSlot.offset = pageRecordInfo.freeSpacePos;
      newSlot.length = sizeInBytes;
      addSlotDirectory(pageRecordInfo, newSlot, pageData);

      pageRecordInfo.numberOfSlots++;
      //			pageRecordInfo.freeSpacePos += sizeInBytes;
      putPageRecordInfo(pageRecordInfo, pageData);
      //			fileHandle.writePage(pageNum, pageData);
      RID newRid;
      newRid.pageNum = pageNum;
      newRid.slotNum = pageRecordInfo.numberOfSlots - 1;
      return newRid;
    }
  }
  // none of the existing pages can fit the record
  //	if (pageNum == fileHandle.getNumberOfPages()) {
  PageRecordInfo pri;
  pri.freeSpacePos = 0;
  pri.numberOfSlots = 0;
  SlotDirectory slot;
  slot.offset = 0;
  slot.length = sizeInBytes;
  addSlotDirectory(pri, slot, pageData);
  pri.numberOfSlots = 1;
  putPageRecordInfo(pri, pageData);
  fileHandle.appendPage(pageData);
  //		recordNo = pri.numberOfSlots - 1;
  //		startPos = slot.offset;
  RID appendPageRid;
  appendPageRid.pageNum = fileHandle.getNumberOfPages() - 1;
  appendPageRid.slotNum = 0;
  // cout << "Appending record in page " << pageNum << "at offset " << startPos << "The length of slot is" << slot.length << endl;
  // cout << "Record Directory Entry: Number Of Records : " << pri.numberOfRecords << endl;
  // cout << "Record Directory Entry: Free Space Pos: " << pri.freeSpacePos << endl;
  //		// cout << "Record Directory Entry: Free Space Pos: " << pri.freeSpacePos << endl;
  return appendPageRid;

  //	}
}

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager *RecordBasedFileManager::instance()
{
  if (!_rbf_manager)
    _rbf_manager = new RecordBasedFileManager();

  return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
  pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
  return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
  return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
                                    FileHandle &fileHandle)
{
  return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
  return pfm->closeFile(fileHandle);
}

r_slot getRecordMetaDataSize(const vector<Attribute> &recordDescriptor)
{
  //
  //  | 2bytes #ofField | 1 byte Tombstone Indicator | 2 bytes per field|

  r_slot returnLength = sizeof(r_slot) + sizeof(char) + sizeof(RID) + sizeof(r_slot) * recordDescriptor.size();
  // cout << "Size of Record meta Data of size " << recordDescriptor.size() << "is " << returnLength;
  return returnLength;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
  if (recordDescriptor.size() == 0)
  {
    return -1;
  }

  r_slot recordSizeInBytes = 0;
  r_slot numberOfFields = recordDescriptor.size();
  r_slot *fieldPointers = new r_slot[numberOfFields];

  // Add 2 byte of space to store number of fields
  recordSizeInBytes += sizeof(r_slot);

  /**
	 * Add the space needed for 1 byte char to express if the record is a tombstone or not
	 * 0 - no tombstone. Actual Data
	 * 1 - tombstone
   * struct RID to store Tombstone indicator pointer
	 */
  char isTombstone = 0;
  recordSizeInBytes += sizeof(char) + sizeof(RID);

  // Add space needed for field pointer array. Field pointers point to start of field
  recordSizeInBytes = recordSizeInBytes + numberOfFields * sizeof(fieldPointers[0]); // 2 byte pointers for each field
  // cout << "Size of field pointer array is " << numberOfFields * sizeof(fieldPointers[0]) << endl;

  // Add space needed for null bytes indicator
  int nullFieldsIndicatorLength = ceil(numberOfFields / 8.0);
  recordSizeInBytes += nullFieldsIndicatorLength;

  r_slot dataOffset = 0;
  unsigned char *nullIndicatorArray = (unsigned char *)malloc(
      nullFieldsIndicatorLength);
  memcpy(nullIndicatorArray, (char *)data + dataOffset,
         nullFieldsIndicatorLength);
  dataOffset += nullFieldsIndicatorLength;

  // find the memory needed to store a record
  for (size_t attri = 0; attri < recordDescriptor.size(); attri++)
  {
    bool isNull = isFieldNull(nullIndicatorArray, attri);
    if (isNull)
    {
      // cout << "Attribute " << attri << " ISNULL: data at offset " << dataOffset << " is null" << endl;
      // cout << "Attribute " << attri << " ISNULL: record offset : " << recordSizeInBytes << endl;
      // cout << "Attribute " << attri << " ISNULL: data offset : " << dataOffset << endl;
      std::bitset<32> nullattbit(
          1 << (nullFieldsIndicatorLength * 8 - 1 - attri));
      // cout << "Test bits " << nullattbit << endl;
      // cout << "Test" << nullIndicatorArray[attri] << endl;
      fieldPointers[attri] = USHRT_MAX;
      continue;
    }
    Attribute recordAttribute = recordDescriptor.at(attri);
    if (recordAttribute.type == TypeInt || recordAttribute.type == TypeReal)
    {
      // cout << "Attribute " << attri << " ISNUMBER: record offset : " << recordSizeInBytes << endl;
      // cout << "Attribute " << attri << " ISNUMBER: data offset : " << dataOffset << endl;
      fieldPointers[attri] = recordSizeInBytes; //point to field start
      recordSizeInBytes += 4;
      dataOffset += 4;
    }
    else
    {
      // cout << "Attribute " << attri << " ISVARCHAR: record offset : " << recordSizeInBytes << endl;
      // cout << "Attribute " << attri << " ISVARCHAR: data offset : " << dataOffset << endl;
      fieldPointers[attri] = recordSizeInBytes; // point to field start
      int varStringLength = 0;
      memcpy(&varStringLength, (char *)data + dataOffset, sizeof(int));
      // cout << "Attribute " << attri << " ISVARCHAR: var string length: " << varStringLength << endl;
      recordSizeInBytes = recordSizeInBytes + sizeof(int) + varStringLength;
      dataOffset = dataOffset + sizeof(int) + varStringLength;
    }
  }
  // dataOffset should store size of incoming data at this point

  // Allocate memory for record data to write to page
  char *record = (char *)malloc(recordSizeInBytes);

  // Copy no. of fields
  int recordOffset = 0;
  memcpy(record, &numberOfFields, sizeof(numberOfFields));
  recordOffset += sizeof(numberOfFields);

  // Copy tombstone indicator
  memcpy(record + recordOffset, &isTombstone, sizeof(isTombstone));
  recordOffset += sizeof(isTombstone);

  //adding tombstone indicator pointer size to offset
  recordOffset += sizeof(RID);

  // Copy field pointers
  memcpy(record + recordOffset, fieldPointers,
         sizeof(fieldPointers[0]) * numberOfFields);
  recordOffset = recordOffset + sizeof(fieldPointers[0]) * numberOfFields;

  // Copy Null indicator array + data. Null indicator array already given in data
  memcpy(record + recordOffset, data, dataOffset);

  //	memcpy(record, data, nullFieldsIndicatorLength);
  //	int offset = nullFieldsIndicatorLength;

  // copy the field values into the record
  //	for (int attri = 0; attri < numberOfFields; attri++) {
  //		bool isNull = nullIndicatorArray[attri]
  //				& (1 << (nullFieldsIndicatorLength * 8 - 1 - attri));
  //		if (isNull) {
  //			continue;
  //		}
  //
  //		Attribute currAttr = recordDescriptor[attri];
  //		if (currAttr.type == TypeVarChar) {
  //			memcpy(record + offset, (char *) data + offset,
  //					currAttr.length + 4);
  //			offset = offset + currAttr.length + 4;
  //		} else {
  //			memcpy(record + offset, (char *) data + offset, currAttr.length);
  //			offset += 4;
  //		}
  //	}

  // search for a page with free space greater than the record size

  char *pageRecordData = (char *)malloc(PAGE_SIZE);
  RID insertRID = getPageForRecordOfSize(fileHandle, recordSizeInBytes,
                                         pageRecordData);

  PageRecordInfo pri;
  getPageRecordInfo(pri, pageRecordData);
  SlotDirectory insertSlot;
  getSlotForRID(pageRecordData, insertRID, insertSlot);

  insertSlot.offset = pri.freeSpacePos;
  insertSlot.length = recordSizeInBytes;
  pri.freeSpacePos += recordSizeInBytes;

  updatePageRecordInfo(pri, pageRecordData);
  updateSlotDirectory(insertRID, pageRecordData, insertSlot);
  //	r_slot startPos =

  memcpy(pageRecordData + insertSlot.offset, record, recordSizeInBytes);
  fileHandle.writePage(insertRID.pageNum, pageRecordData);

  // Now read the record dictionary and find the first empty slot for insertion
  //	fileHandle.readPage(pageNum, pageRecordData);
  //	dict = reinterpret_cast<RecordDictionary*>(pageRecordData);
  //	memcpy(&dict, pageRecordData, sizeof(struct RecordDictionary));
  //	unsigned int newRecordStart = dict.freeSpacePos;
  //	unsigned char newRecordNum = dict.numberOfRecords;
  //
  //	// Then update the dictionary and insert record at freeSpacePointer
  //	dict.recordInfo[newRecordNum].startPos = newRecordStart;
  //	dict.recordInfo[newRecordNum].offset = recordSizeInBytes;
  //	dict.freeSpacePos = newRecordStart + (unsigned int) recordSizeInBytes;
  //	dict.numberOfRecords++;
  //
  //	memcpy(pageRecordData, &dict, sizeof(struct RecordDictionary));
  //	memcpy(pageRecordData + newRecordStart, record, recordSizeInBytes);
  //	fileHandle.writePage(pageNum, pageRecordData);
  //
  // update the new rid for the record
  rid.pageNum = insertRID.pageNum;
  rid.slotNum = insertRID.slotNum;
  // return 0

  free(record);
  record = NULL;
  //	free(nullIndicatorArray);
  free(pageRecordData);
  pageRecordData = NULL;

  return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
  char *pageData = (char *)malloc(PAGE_SIZE);
  fileHandle.readPage(rid.pageNum, pageData);

  SlotDirectory slot;
  getSlotForRID(pageData, rid, slot);

  // If trying to read a deleted record return failure
  if (slot.offset == USHRT_MAX)
  {
    return failure;
  }
  // cout << "Reading Data" << endl;
  // cout << "For slot " << rid.slotNum << " : Slot offset from file is "<<  slot.offset << ". And slot length is " << slot.length << endl;
  PageRecordInfo pri;
  getPageRecordInfo(pri, (char *)pageData);

  char isTombstone;
  memcpy(&isTombstone, pageData + slot.offset + 4, 1);
  if (isTombstone == 1)
  {
    RID newRID;
    memcpy(&newRID, pageData + slot.offset + 5, sizeof(newRID));
    return readRecord(fileHandle, recordDescriptor, newRID, data);
  }
  // cout << "Record Directory : Number of records : " << pri.numberOfRecords <<". Free Space : " << pri.freeSpacePos << endl;
  r_slot recordMetaDataLength = getRecordMetaDataSize(recordDescriptor);
  memcpy(data, pageData + slot.offset + recordMetaDataLength,
         slot.length - recordMetaDataLength);
  free(pageData);
  pageData = NULL;
  return success;
}

RC RecordBasedFileManager::printRecord(
    const vector<Attribute> &recordDescriptor, const void *data)
{
  size_t nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
  unsigned char *nullIndicator = (unsigned char *)malloc(nullIndicatorSize);
  memcpy(nullIndicator, data, nullIndicatorSize);
  size_t offset = nullIndicatorSize;
  for (unsigned int attri = 0; attri < recordDescriptor.size(); attri++)
  {
    Attribute attribute = recordDescriptor[attri];
    bool isNull = isFieldNull(nullIndicator, attri);
    if (isNull)
    {
      printf("%s : NULL ", attribute.name.c_str());
      continue;
    }
    if (attribute.type == TypeVarChar)
    {
      int length = 0;
      memcpy(&length, (char *)data + offset, sizeof(int));
      char *content = (char *)malloc(length);
      memcpy(content, (char *)data + offset + sizeof(int), length);
      offset = offset + sizeof(int) + length;
      printf("%s : %s ", attribute.name.c_str(), content);
      free(content);
    }
    else if (attribute.type == TypeInt)
    {
      int integerData = 0;
      memcpy(&integerData, (char *)data + offset, sizeof(int));
      offset = offset + sizeof(int);
      printf("%s : %d ", attribute.name.c_str(), integerData);
    }
    else
    {
      float realData = 0;
      memcpy(&realData, (char *)data + offset, sizeof(int));
      offset = offset + sizeof(float);
      printf("%s : %f ", attribute.name.c_str(), realData);
    }
  }
  return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor, const RID &rid)
{

  PageNum pageNum = rid.pageNum;

  // read page from which to delete record
  char *pageData = (char *)malloc(PAGE_SIZE);
  fileHandle.readPage(pageNum, pageData);

  // get the record directory information
  PageRecordInfo pri;
  getPageRecordInfo(pri, pageData);

  // get the slot directory
  SlotDirectory slotToDelete;
  getSlotForRID(pageData, rid, slotToDelete);
  r_slot lengthOfDeletedRecord = slotToDelete.length;

  // Are you deleting the last record in the page
  if (rid.slotNum + 1 == pri.numberOfSlots)
  {
    slotToDelete.offset = USHRT_MAX;
    updateSlotDirectory(rid, pageData, slotToDelete);

    pri.freeSpacePos -= lengthOfDeletedRecord;
    updatePageRecordInfo(pri, pageData);

    RC writeStatus = fileHandle.writePage(pageNum, pageData);
    return writeStatus;
  }
  else
  {
    // for each consecutive record
    slotToDelete.offset = USHRT_MAX;
    updateSlotDirectory(rid, pageData, slotToDelete);

    //		pri.numberOfRecords -= 1;
    pri.freeSpacePos -= lengthOfDeletedRecord;
    updatePageRecordInfo(pri, pageData);

    for (r_slot islot = rid.slotNum + 1; islot < pri.numberOfSlots;
         islot++)
    {
      RID ridOfRecordToShift;
      ridOfRecordToShift.pageNum = pageNum;
      ridOfRecordToShift.slotNum = islot;

      // shift record to left
      shiftRecord(pageData, ridOfRecordToShift, -lengthOfDeletedRecord);
    }

    RC writeStatus = fileHandle.writePage(pageNum, pageData);
    return writeStatus;
  }

  // get the slot directory
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor, const void *data,
                                        const RID &rid)
{
  // Get the page where the record is present
  int pageNum = rid.pageNum;
  char *pageData = (char *)malloc(PAGE_SIZE);
  fileHandle.readPage(pageNum, pageData);

  // Get the rid slot where the record is located
  SlotDirectory slot;
  getSlotForRID(pageData, rid, slot);

  //get page record info
  PageRecordInfo pageRecordInfo;
  getPageRecordInfo(pageRecordInfo, pageData);
  //get free space in file
  r_slot freeSpaceAvailable = PAGE_SIZE - getRecordDirectorySize(pageRecordInfo) //slots + pageRecordInfo
                              - (pageRecordInfo.freeSpacePos);

  //Transform raw Data into record format
  char *record = (char *)malloc(PAGE_SIZE);
  // if length of new record data < length of old record data
  r_slot newRecordLength = getLengthOfRecordAndTransformRecord(data, recordDescriptor, record);

  short oldLength = slot.length;
  short lengthDiff = oldLength - newRecordLength;
  if (newRecordLength < slot.length)
  {
    // place record at offset of old record, slot.offset
    memmove(pageData + slot.offset, record, newRecordLength);

    // update the length of old slot with new slot,
    slot.length = newRecordLength;
    updateSlotDirectory(rid, pageData, slot);

    pageRecordInfo.freeSpacePos -= (lengthDiff);
    updatePageRecordInfo(pageRecordInfo, pageData);

    for (r_slot islot = rid.slotNum + 1; islot < pageRecordInfo.numberOfSlots;
         islot++)
    {
      RID ridOfRecordToShift;
      ridOfRecordToShift.pageNum = pageNum;
      ridOfRecordToShift.slotNum = islot;

      // shift record to left
      shiftRecord(pageData, ridOfRecordToShift, -lengthDiff);
    }
  }
  else if (newRecordLength > slot.length)
  {
    if (freeSpaceAvailable >= newRecordLength - slot.length)
    {
      for (r_slot islot = pageRecordInfo.numberOfSlots - 1; islot > rid.slotNum;
           islot--)
      {
        RID ridOfRecordToShift;
        ridOfRecordToShift.pageNum = pageNum;
        ridOfRecordToShift.slotNum = islot;

        // shift record to right
        shiftRecord(pageData, ridOfRecordToShift, -lengthDiff);
      }

      // place record at offset of old record, slot.offset
      memmove(pageData + slot.offset, record, newRecordLength);

      // update the length of old slot with new slot,
      slot.length = newRecordLength;
      updateSlotDirectory(rid, pageData, slot);

      pageRecordInfo.freeSpacePos += (-lengthDiff);
      updatePageRecordInfo(pageRecordInfo, pageData);
    }
    else
    {
      //Update the tombstone to 1
      char isTombstone = 1;
      memmove(pageData + slot.offset + SLOT_SIZE, &isTombstone, sizeof(char));
      //Move record to next available page and leave tombstone
      RID newRID;
      insertRecord(fileHandle, recordDescriptor, data, newRID);
      //Update the tombstone indicator pointer
      memmove(pageData + slot.offset + SLOT_SIZE + sizeof(char), &newRID, sizeof(RID));

      for (r_slot islot = rid.slotNum + 1; islot < pageRecordInfo.numberOfSlots;
           islot++)
      {
        RID ridOfRecordToShift;
        ridOfRecordToShift.pageNum = pageNum;
        ridOfRecordToShift.slotNum = islot;

        // shift record to left
        shiftRecord(pageData, ridOfRecordToShift, -(oldLength - (sizeof(r_slot) + sizeof(char) + sizeof(RID))));
      }
    }
  }
  else if (newRecordLength == slot.length)
  {
    // place record at offset of old record, slot.offset
    memmove(pageData + slot.offset, record, newRecordLength);
  }

  RC writeStatus = fileHandle.writePage(pageNum, pageData);

  free(pageData);
  free(record);
  pageData = NULL;
  record = NULL;
  return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
                                const void *value,                                     // used in the comparison
                                const vector<string> &attributeNames,                  // a list of projected attributes
                                RBFM_ScanIterator &rbfm_ScanIterator)
{
  rbfm_ScanIterator.fileHandle = &fileHandle;
  rbfm_ScanIterator.compOp = compOp;
  rbfm_ScanIterator.attributeNames = attributeNames;
  rbfm_ScanIterator.recordDescriptor = recordDescriptor;
  rbfm_ScanIterator.conditionAttribute = conditionAttribute;
  rbfm_ScanIterator.value = value;

  rbfm_ScanIterator.nextRID.pageNum = 0;
  rbfm_ScanIterator.nextRID.slotNum = 0;
  rbfm_ScanIterator.isEOF = 0;

  return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
                                         const vector<Attribute> &recordDescriptor, const RID &rid,
                                         const string &attributeName, void *data)
{

  char *pageData = (char *)malloc(PAGE_SIZE);
  fileHandle.readPage(rid.pageNum, pageData);

  SlotDirectory recordSlot;
  getSlotForRID(pageData, rid, recordSlot);

  char *recordData = (char *)malloc(recordSlot.length);
  memcpy(recordData, pageData + recordSlot.offset, recordSlot.length);

  Record record = Record(recordDescriptor, recordData);
  char *attributeValue = record.getAttributeValue(attributeName);

  // Add a one byte null indicator array always for read record
  unsigned char *nullIndicatorArray = (unsigned char *)malloc(1);
  memset(nullIndicatorArray, 0, 1);
  if (attributeValue == NULL)
  {
    makeFieldNull(nullIndicatorArray, 0);
  }
  memcpy(data, nullIndicatorArray, 1);
  AttrType attributeType = record.getAttributeType(attributeName);
  // if (attributeType == TypeVarChar)
  // {
  //   unsigned attributeLength = record.getAttributeLength(attributeName);
  //   memcpy((char *)data + valueOffset, &attributeLength, 4);
  //   valueOffset += 4;
  // }
  memcpy((char *)data + 1, attributeValue, strlen(attributeValue));

  free(pageData);
  free(recordData);
  return success;
}

void Record::setNumberOfFields()
{
  memcpy(&numberOfFields, recordData, sizeof(numberOfFields));
}

void Record::setTombstoneIndicator()
{
  memcpy(&tombstoneIndicator, recordData + sizeof(numberOfFields),
         sizeof(tombstoneIndicator));
}

void Record::setTombstoneIndicatorPtr()
{
  memcpy(&tombstoneRID, recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator),
         sizeof(RID));
}

void Record::setFieldPointers()
{
  fieldPointers = new r_slot[numberOfFields];
  memcpy(fieldPointers,
         recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator) + sizeof(tombstoneRID),
         sizeof(r_slot) * numberOfFields);
}

void Record::setInputData()
{
  r_slot sizeOfInputData = getRawRecordSize();
  inputData = (char *)malloc(sizeOfInputData);
  memcpy(inputData,
         recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator) + sizeof(tombstoneRID) + sizeof(r_slot) * numberOfFields, sizeOfInputData);
}

void Record::setNullIndicatorArray()
{
  r_slot sizeOfNullIndicatorArray = 0;
  sizeOfNullIndicatorArray = ceil(numberOfFields / 8.0);
  nullIndicatorArray = (unsigned char *)malloc(sizeOfNullIndicatorArray);

  memcpy(nullIndicatorArray,
         recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator) + sizeof(tombstoneRID) + sizeof(r_slot) * numberOfFields,
         sizeOfNullIndicatorArray);
}

r_slot Record::getRawRecordSize()
{
  r_slot lastFieldPointer = fieldPointers[numberOfFields - 1];
  AttrType lastRecordType = recordDescriptor[numberOfFields - 1].type;
  int lastFieldLength = 0;
  if (lastRecordType == TypeVarChar)
  {
    memcpy(&lastFieldLength, recordData + lastFieldPointer,
           sizeof(lastFieldLength));
    lastFieldLength += 4;
  }
  else
  {
    lastFieldLength = 4;
  }
  r_slot sizeOfInputData = lastFieldPointer + lastFieldLength - fieldPointers[0];
  return sizeOfInputData;
}

Record::Record(const vector<Attribute> &recordDesc,
               char *const dataOfStoredRecord)
{
  recordDescriptor = recordDesc;
  recordData = dataOfStoredRecord;

  setNumberOfFields();
  setTombstoneIndicator();
  setTombstoneIndicatorPtr();
  setFieldPointers();
  setInputData();
  setNullIndicatorArray();
}
//	~Record(){
//		delete rawData;
//		delete recordData;
//		rawData = NULL;
//		recordData = NULL;
//	}

r_slot Record::getNumberOfFields()
{
  return numberOfFields;
}

r_slot Record::getRecordSize()
{

  return getRawRecordSize() + getRecordMetaDataSize(recordDescriptor);
}

char *Record::getAttributeValue(const string &attributeName)
{
  r_slot fieldPointerIndex = 0;
  for (Attribute a : recordDescriptor)
  {
    if (a.name.compare(attributeName) == 0)
    {
      break;
    }
    else
    {
      fieldPointerIndex++;
    }
  }
  return getAttributeValue(fieldPointerIndex);
}

bool Record::isTombstone()
{
  return (tombstoneIndicator == 1);
}

char *Record::getAttributeValue(r_slot fieldNumber)
{
  if (isFieldNull(fieldNumber))
  {
    return NULL;
  }
  r_slot fieldStartPointer = fieldPointers[fieldNumber];
  char *attributeValue = NULL;
  Attribute attributeMetaData = recordDescriptor[fieldNumber];
  if (attributeMetaData.type == TypeVarChar)
  {
    int lengthOfString = 0;
    memcpy(&lengthOfString, recordData + fieldStartPointer,
           sizeof(lengthOfString));
    attributeValue = (char *)malloc(lengthOfString);
    memcpy(attributeValue,
           recordData + fieldStartPointer + sizeof(lengthOfString),
           lengthOfString);
  }
  else
  {
    memcpy(attributeValue, recordData + fieldStartPointer, 4);
  }
  return attributeValue;
}

AttrType Record::getAttributeType(const string &attributeName)
{
  for (Attribute a : recordDescriptor)
  {
    if (a.name.compare(attributeName) == 0)
    {
      return a.type;
    }
  }
  return TypeInt; //TODO: Cerr
}

bool Record::isFieldNull(r_slot fieldIndex)
{

  int byteNumber = fieldIndex / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - fieldIndex % 8));
  return isNull;
}

////// ScanIterator

RBFM_ScanIterator::RBFM_ScanIterator()
{
  isEOF = 0;
  conditionAttribute = "";
  nextRID.pageNum = 0;
  nextRID.slotNum = 0;
  value = NULL;
  compOp = NO_OP;
  fileHandle = NULL;
}

bool CheckCondition(AttrType conditionAttributeType, char *attributeValue, const void *value, CompOp compOp)
{
  //Removing 1 byte of nullIndicator from value
  unsigned char *valueNullIndicator = (unsigned char *)malloc(1);
  memcpy(valueNullIndicator, value, 1);
  bool isValueNull = isFieldNull(valueNullIndicator, 0);

  if (!isValueNull)
  {
    unsigned valueLength = strlen((char *)value + 1);
    char *conditionValue = (char *)malloc(valueLength);
    memcpy(conditionValue, (char *)value + 1, valueLength);

    if (conditionAttributeType == TypeVarChar)
    {
      switch (compOp)
      {
      case EQ_OP:
        if (strcmp(attributeValue, (const char *)conditionValue) == 0)
          return true;
        break;
      case LT_OP:
        if (strcmp(attributeValue, (const char *)conditionValue) < 0)
          return true;
        break;
      case LE_OP:
        if (strcmp(attributeValue, (const char *)conditionValue) <= 0)
          return true;
        break;
      case GT_OP:
        if (strcmp(attributeValue, (const char *)conditionValue) > 0)
          return true;
        break;
      case GE_OP:
        if (strcmp(attributeValue, (const char *)conditionValue) >= 0)
          return true;
        break;
      case NE_OP:
        if (strcmp(attributeValue, (const char *)conditionValue) != 0)
          return true;
        break;
      case NO_OP:
        return true;
      }
    }
    else if (conditionAttributeType == TypeInt)
    {
      int attrValue = atoi(attributeValue);
      int compValue = 0;
      memcpy(&compValue, conditionValue, sizeof(int));
      switch (compOp)
      {
      case EQ_OP:
        if (attrValue == compValue)
          return true;
        break;
      case LT_OP:
        if (attrValue < compValue)
          return true;
        break;
      case LE_OP:
        if (attrValue <= compValue)
          return true;
        break;
      case GT_OP:
        if (attrValue > compValue)
          return true;
        break;
      case GE_OP:
        if (attrValue >= compValue)
          return true;
        break;
      case NE_OP:
        if (attrValue != compValue)
          return true;
        break;
      case NO_OP:
        return true;
        break;
      }
    }
    else if (conditionAttributeType == TypeReal)
    {
      float attrValue = strtof(attributeValue, 0);
      float compValue = 0.0;
      memcpy(&compValue, conditionValue, sizeof(float));
      switch (compOp)
      {
      case EQ_OP:
        if (attrValue == compValue)
          return true;
        break;
      case LT_OP:
        if (attrValue < compValue)
          return true;
        break;
      case LE_OP:
        if (attrValue <= compValue)
          return true;
        break;
      case GT_OP:
        if (attrValue > compValue)
          return true;
        break;
      case GE_OP:
        if (attrValue >= compValue)
          return true;
        break;
      case NE_OP:
        if (attrValue != compValue)
          return true;
        break;
      case NO_OP:
        return true;
        break;
      }
    }
  }
  else
  {
    switch (compOp)
    {
    case EQ_OP:
      if (attributeValue == NULL)
        return true;
      else
        return false;
      break;
    case NE_OP:
      if (attributeValue != NULL)
        return true;
      else
        return false;
      break;
    default:
      return false;
      break;
    }
  }

  return false;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
  if (isEOF == RBFM_EOF)
    return -1;

  rid = nextRID;
  bool hitFound = false;
  void *recordData = (char *)malloc(PAGE_SIZE);

  while (!hitFound)
  {
    char *pageData = (char *)malloc(PAGE_SIZE);
    fileHandle->readPage(rid.pageNum, pageData);
    SlotDirectory slot;
    getSlotForRID(pageData, rid, slot);

    memcpy(recordData, pageData + slot.offset, slot.length);
    Record record = Record(recordDescriptor, (char *)recordData);

    char *attributeValue;
    attributeValue = record.getAttributeValue(conditionAttribute);

    AttrType conditionAttributeType = record.getAttributeType(conditionAttribute);
    if (CheckCondition(conditionAttributeType, attributeValue, value, compOp))
    {
      hitFound = true;
    }

    PageRecordInfo pri;
    getPageRecordInfo(pri, pageData);
    if (rid.slotNum + 1 > pri.numberOfSlots)
    {
      unsigned numberOfPages = fileHandle->getNumberOfPages();
      if (rid.pageNum >= numberOfPages - 1)
      {
        isEOF = RBFM_EOF;
        nextRID.pageNum = 0;
        nextRID.slotNum = 0;
        return 0;
      }
      else
      {
        rid.pageNum++;
        rid.slotNum = 0;
      }
    }
    else
      rid.slotNum++;

    free(pageData);
  }

  if (hitFound)
  {
    r_slot numberOfFields = 0;
    memcpy(&numberOfFields, (char *)recordData, sizeof(numberOfFields));
    r_slot *fieldPointers = new r_slot[numberOfFields];
    memcpy(&fieldPointers,
           (char *)recordData + sizeof(numberOfFields) + sizeof(char) + sizeof(RID),
           sizeof(r_slot) * numberOfFields);
    int nullFieldsIndicatorLength = ceil(numberOfFields / 8.0);
    unsigned char *nullIndicatorArray = (unsigned char *)malloc(nullFieldsIndicatorLength);
    memcpy(nullIndicatorArray, (char *)recordData + sizeof(numberOfFields) + sizeof(char) + sizeof(RID) + (numberOfFields * sizeof(fieldPointers[0])),
           nullFieldsIndicatorLength);

    short attrNumberOfFields = attributeNames.size();
    int attrNullFieldsIndicatorLength = ceil(attrNumberOfFields / 8.0);
    unsigned char *attrNullIndicatorArray = (unsigned char *)malloc(attrNullFieldsIndicatorLength);

    r_slot attrFieldPointerIndex = 0;
    int attrOffset = attrNullFieldsIndicatorLength;
    for (Attribute a : recordDescriptor)
    {
      for (string s : attributeNames)
      {
        if (a.name.compare(s) == 0)
        {
          char nullBit = isFieldNull(nullIndicatorArray, attrFieldPointerIndex) ? 0 : 1;
          int byteNumber = attrFieldPointerIndex / 8;
          attrNullIndicatorArray[byteNumber] |= (nullBit << (7 - attrFieldPointerIndex % 8));
          if (a.type == TypeVarChar)
          {
            int varcharLength = 0;
            memcpy(&varcharLength, (char *)recordData + fieldPointers[attrFieldPointerIndex], 4);
            memcpy((char *)data + attrOffset, (char *)recordData + fieldPointers[attrFieldPointerIndex], 4 + varcharLength);
            attrOffset += (4 + varcharLength);
          }
          else
          {
            memcpy((char *)data + attrOffset, (char *)recordData + fieldPointers[attrFieldPointerIndex], 4);
            attrOffset += 4;
          }
        }
        attrFieldPointerIndex++;
      }
    }
    memcpy((char *)data, attrNullIndicatorArray, attrNullFieldsIndicatorLength);
  }

  free(recordData);
  return 0;
}

RC RBFM_ScanIterator::close()
{

  return 0;
}

RawRecordPreparer::RawRecordPreparer(
    const vector<Attribute> &recordDescriptor)
{
  this->recordDescriptor = recordDescriptor;
  nullIndicatorArraySize = ceil(recordDescriptor.size() / 8.0);
  nullIndicatorArray = new unsigned char[nullIndicatorArraySize];
  memset(nullIndicatorArray, 0, nullIndicatorArraySize);
  for (Attribute atr : recordDescriptor)
  {
    currentRecordSize += atr.length + nullIndicatorArraySize;
  }
  recordData = (char *)malloc(currentRecordSize);
  recordDataOffset += nullIndicatorArraySize;
}

char *RawRecordPreparer::prepareRecord()
{
  // check if all field values have been passed
  if (fieldIndexCounter != recordDescriptor.size())
  {
    cout << "Record - Schema mismatch. Missing fields. Exiting!" << endl;
    exit(1);
  }
  memcpy(recordData, nullIndicatorArray, nullIndicatorArraySize);
  // shrink the recordData size if not needed
  if (recordDataOffset < currentRecordSize)
  {
    char *temp = (char *)realloc(recordData, recordDataOffset);
    if (temp == NULL)
    {
      cout << "error reallocating recordData. Exiting";
      exit(1);
    }

    recordData = temp;
    currentRecordSize = recordDataOffset;
  }
  // set another pointer to recordData
  char *returnRecordData = recordData;
  // reset recordData and other variables so that next record can be prepared
  resetCounters();

  return returnRecordData;
}

RawRecordPreparer &RawRecordPreparer::setField(const string &value)
{
  Attribute attr = recordDescriptor[fieldIndexCounter];
  if (attr.type != TypeVarChar)
  {
    cout << "Error in field setting : Expecting " << attr.name << " to be of type " << attr.type << " but got string";
    exit(1);
  }
  else if (!isValidField())
  {
    cout << "Field index out of bounds" << fieldIndexCounter << endl;
  }
  else
  {
    resizeRecordDataIfNeeded(value.size() + 4);
    int sizeOfString = value.size();
    memcpy(recordData + recordDataOffset, &sizeOfString, sizeof(int));
    recordDataOffset += sizeof(int);
    memcpy(recordData + recordDataOffset, value.c_str(), value.size());
    recordDataOffset += value.size();
  }
  fieldIndexCounter++;
  return *this;
}

RawRecordPreparer &RawRecordPreparer::setField(int value)
{
  Attribute attr = recordDescriptor[fieldIndexCounter];
  if (attr.type != TypeInt)
  {
    cout << "Error in field setting : Expecting " << attr.name << " to be of type " << attr.type << " but got 'int'";
    exit(1);
  }
  else if (!isValidField())
  {
    cout << "Field index out of bounds" << fieldIndexCounter << endl;
  }
  else
  {
    resizeRecordDataIfNeeded(sizeof(value));
    memcpy(recordData + recordDataOffset, &value, sizeof(value));
    recordDataOffset += sizeof(value);
  }
  fieldIndexCounter++;
  return *this;
}

RawRecordPreparer &RawRecordPreparer::setField(float value)
{
  Attribute attr = recordDescriptor[fieldIndexCounter];
  if (attr.type != TypeReal)
  {
    cout << "Error in field setting : Expecting " << attr.name << " to be of type " << attr.type << " but got 'float'";
    exit(1);
  }
  else if (!isValidField())
  {
    cout << "Field index out of bounds" << fieldIndexCounter << endl;
  }
  else
  {
    resizeRecordDataIfNeeded(sizeof(value));
    memcpy(recordData + recordDataOffset, &value, sizeof(value));
    recordDataOffset += sizeof(value);
  }
  fieldIndexCounter++;
  return *this;
}

void RawRecordPreparer::resizeRecordDataIfNeeded(int size)
{
  if (size + recordDataOffset <= currentRecordSize)
  {
    return;
  }
  char *temp = (char *)realloc(recordData, currentRecordSize + size);
  if (temp == NULL)
  {
    cout << "memory reallocation failed";
    exit(1);
  }
  else
  {
    recordData = temp;
    currentRecordSize += size;
  }
}

RawRecordPreparer &RawRecordPreparer::setNull()
{

  if (isValidField())
  {
    //		nullIndicatorArray[fieldIndexCounter]=1;
    makeFieldNull(nullIndicatorArray, fieldIndexCounter);
    fieldIndexCounter++;
  }
  else
  {
    cout << "Field Index out of bounds" << fieldIndexCounter << endl;
    cout << "Exiting";
    exit(1);
  }
  return *this;
}

bool RawRecordPreparer::isValidField()
{
  if (fieldIndexCounter < recordDescriptor.size())
  {
    return true;
  }
  else
  {
    return false;
  }
}

RawRecordPreparer &RawRecordPreparer::setRecordDescriptor(
    const vector<Attribute> &recordDescriptor)
{
  this->recordDescriptor = recordDescriptor;
  return *this;
}

RawRecordPreparer::~RawRecordPreparer()
{
  free(nullIndicatorArray);
  free(recordData);
  recordData = NULL;
  nullIndicatorArray = NULL;
}

void RawRecordPreparer::resetCounters()
{
  // set all values to zero initially
  recordDataOffset = 0;
  fieldIndexCounter = 0;
  currentRecordSize = 0;
  recordData = NULL;
  nullIndicatorArray = NULL;
  nullIndicatorArraySize = 0;

  // set it as the constructor initializes the values
  nullIndicatorArraySize = ceil(recordDescriptor.size() / 8.0);
  nullIndicatorArray = new unsigned char[nullIndicatorArraySize];
  memset(nullIndicatorArray, 0, nullIndicatorArraySize);
  for (Attribute atr : recordDescriptor)
  {
    currentRecordSize += atr.length + nullIndicatorArraySize;
  }
  recordData = (char *)malloc(currentRecordSize);
  recordDataOffset += nullIndicatorArraySize;
}
