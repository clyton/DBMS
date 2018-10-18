#include <ext/type_traits.h>
#include <rbf/rbfm.h>
#include <stddef.h>
#include <stdlib.h>
#include <bitset>
#include <cmath>
#include <cstdio>
#include <cstring>

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

bool isFieldNull(unsigned char *nullIndicatorArray, int fieldIndex)
{
  int byteNumber = fieldIndex / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - fieldIndex % 8));
  return isNull;
}

r_slot getLengthOfRecord(const void *data,
                         const vector<Attribute> &recordDescriptor)
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
	 */
  char isTombstone = 0;
  recordSizeInBytes += sizeof(char);

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

  r_slot returnLength = sizeof(r_slot) + sizeof(char) + sizeof(r_slot) * recordDescriptor.size();
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
	 */
  char isTombstone = 0;
  recordSizeInBytes += sizeof(char);

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

  // if length of new record data < length of old record data
  r_slot newRecordLength = getLengthOfRecord(data, recordDescriptor);
  if (newRecordLength <= slot.length)
  {
    // place record at offset of old record, slot.offset
    memcpy(pageData + slot.offset, data, newRecordLength);

    // update the length of old slot with new slot,
    short oldLength = slot.length;
    slot.length = newRecordLength;

    // memmove(page+slot.offset+slot.length, page+ slot.offset+oldLength, FreeSpacePointer - (slot.offset + oldLength))
    memmove(page + slot.offset + slot.length, page + slot.offset + oldLength, FreeSpacePointer - (slot.offset + oldLength))
    // for each slot after the currently updated slot

    // slot.offset = slot.offset - (oldLength - newLength);
  }
  else if (newRecordLength > slot.length)
  {
    if (freeSpaceAvailable >= newRecordLength - slot.length)
    {
    }
    else
    {
    }
    //Check if (freeSpace >= (newLength - oldLength))
    //shift the records after the old record by (newLength - oldLength)
    //memmove new record over (old record + freed up space)
    //update each slot offset of shifted record
    //update freeSpaceOffset = freeSpaceOffset + (newLength - oldLength)
  }
  else if ()
  {
  }

  //else if length of new record data > length of old record data && (newLength - oldLength) < freeSpaceAvailable
  // delta <- |newLength - oldLength|
  // currentSlot <- page.getSlot(rid.slot)
  // nextSlot <- page.getSlot(rid.slot+1)

  // memmove(page+nextSlot.offset + delta, page + nextSlot.offset, FreeSpacePointer - nextSlot.offset)
  // for(all slots from nextSlot to endSlot)
  // islot.offset= islot.offset + delta;
  // else
  // insertRid <- insertRecord(data, recordDescriptor)
  // Record oldRecord
  // void* oldRecordData
  // readRecord(oldRecordData, recordDescriptor, rid)
  // read(oldRecord, oldRecordData) ... cast data to Record object
  // oldRecord.makeTombstone(insertRid)
  // currentSlot <- page.getSlot(rid.slot)
  // nextSlot <- page.getSlot(rid.slot + 1)
  // set the tombstone byte to 1
  // memcpy(oldRecord.getTombstoneEnd + 1, insertRid.pageNum, sizeof(r_slot))
  // memcpy(oldRecord.getTombstoneEnd + 3, insertRid.slotNum, sizeof(r_slot))
  // memmove(page + currentSlot + tombstoneRecordLength,  page + nextSlot.offset, currentSlot.length - tombstoneRecordLength)
  // for(all slots from nextslot to endSlot
  // islot.offset = islot.offset + currentSlot.length - tombstoneRecordLength

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
  string attributeValue = record.getAttributeValue(attributeName);
  memcpy(data, &attributeValue, attributeValue.size());

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

void Record::setFieldPointers()
{
  fieldPointers = new r_slot[numberOfFields];
  memcpy(&fieldPointers,
         recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator),
         sizeof(r_slot) * numberOfFields);
}

void Record::setInputData()
{
  r_slot sizeOfInputData = getRawRecordSize();
  memcpy(inputData,
         recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator) + sizeof(r_slot) * numberOfFields, sizeOfInputData);
}

void Record::setNullIndicatorArray()
{
  r_slot sizeOfNullIndicatorArray = 0;
  sizeOfNullIndicatorArray = ceil(numberOfFields / 8.0);

  memcpy(nullIndicatorArray,
         recordData + sizeof(numberOfFields) + sizeof(tombstoneIndicator) + sizeof(r_slot) * numberOfFields,
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

string Record::getAttributeValue(const string &attributeName)
{
  r_slot fieldPointerIndex = 0;
  for (Attribute a : recordDescriptor)
  {
    if (a.name.compare(attributeName))
    {
      break;
    }
    fieldPointerIndex++;
  }

  return getAttributeValue(fieldPointerIndex);
}

string Record::getAttributeValue(r_slot fieldNumber)
{
  if (isFieldNull(fieldNumber))
  {
    return NULL;
  }
  r_slot fieldStartPointer = fieldPointers[fieldNumber];
  string attributeValue = "";
  Attribute attributeMetaData;
  if (attributeMetaData.type == TypeVarChar)
  {
    int lengthOfString = 0;
    memcpy(&lengthOfString, recordData + fieldStartPointer,
           sizeof(lengthOfString));
    memcpy(&attributeValue,
           recordData + fieldStartPointer + sizeof(lengthOfString),
           lengthOfString);
  }
  else
  {
    memcpy(&attributeValue, recordData + fieldStartPointer, 4);
  }
  return attributeValue;
}

bool Record::isTombstone()
{
  return (tombstoneIndicator == 1);
}

bool Record::isFieldNull(r_slot fieldIndex)
{

  int byteNumber = fieldIndex / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - fieldIndex % 8));
  return isNull;
}
