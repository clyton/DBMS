
#include "qe.h"

#include <stddef.h>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "../rbf/pfm.h"

int getSizeForNullIndicator(int fieldCount) {
  return ceil((double)fieldCount / CHAR_BIT);
}

Filter::Filter(Iterator* input, const Condition& condition) {
  this->input = input;
  this->condition = condition;
  getAttributes(attributes);
  this->cEval = new ConditionEvaluator(condition, attributes);
}

RC Filter::getNextTuple(void* data) {
  RC isEOF = false;
  while (true) {
    isEOF = input->getNextTuple(data);
    RawRecord dataRecord((char*)data, attributes);
    if (isEOF != QE_EOF) {
      if (cEval->evaluateFor(dataRecord)) {
        return (isEOF);
      }
    } else {
      return (isEOF);
    }
  }
}

void Filter::getAttributes(vector<Attribute>& attrs) const {
  input->getAttributes(attrs);
}

RawRecord::RawRecord(char* rawRecord, vector<Attribute>& attrs) {
  this->rawRecord = rawRecord;
  this->attributes = attrs;
  setUpAttributeValue();
}
// ... the rest of your implementations go here

const vector<Attribute>& RawRecord::getAttributes() const {
  return (this->attributes);
}

char* RawRecord::getBuffer() const { return (rawRecord); }

void RawRecord::setUpAttributeValue() {
  int size = getNullIndicatorSize();

  int offset = size;
  for (size_t i = 0; i < attributes.size(); ++i) {
    Value value;
    value.type = attributes[i].type;
    if (isFieldNull(i)) {
      value.data = nullptr;
      attributeValue.push_back(value);
      continue;
    }
    // if the value is not null
    value.data = new char[attributes[i].length]();

    switch (value.type) {
      case TypeInt:
        memcpy(&value.data, rawRecord + offset, sizeof(int));
        offset += sizeof(int);
        break;
      case TypeReal:
        memcpy(&value.data, rawRecord + offset, sizeof(int));
        offset += sizeof(int);
        break;
      case TypeVarChar:
        int length;
        memcpy(&length, rawRecord + offset, sizeof(int));
        memcpy(&value.data, rawRecord + offset, sizeof(length) + length);
        offset += sizeof(length) + length;
        break;
    }

    attributeValue.push_back(value);
  }
}

Value& RawRecord::getAttributeValue(int index) {
  return (attributeValue[index]);
}

Value& RawRecord::getAttributeValue(Attribute& attribute) {
  for (size_t i = 0; i < attributes.size(); ++i) {
    if (attributes[i].name.compare(attribute.name) == 0)
      return (getAttributeValue(i));
  }
  cerr << "Wrong attribute name";
  exit(1);
}

Value& RawRecord::getAttributeValue(string& attributeName) {
  for (size_t i = 0; i < attributes.size(); ++i) {
    if (attributes[i].name.compare(attributeName) == 0)
      return (getAttributeValue(i));
  }
  cerr << "Wrong attribute name";
  exit(1);
}

ConditionEvaluator::ConditionEvaluator(const Condition& condition,
                                       vector<Attribute>& attrs) {
  this->condition = condition;
  this->attributes = attrs;
}

bool ConditionEvaluator::evaluateFor(RawRecord& rawRecord) {
  CompOp compOp = condition.op;
  char* value;
  Value lhsValue = rawRecord.getAttributeValue(condition.lhsAttr);

  if (condition.bRhsIsAttr) {
    value = (char*)rawRecord.getAttributeValue(condition.rhsAttr).data;
  } else {
    value = (char*)condition.rhsValue.data;
  }
  return (CheckCondition(lhsValue.type, (char*)lhsValue.data, value, compOp));
}

bool RawRecord::isFieldNull(int index) {
  unsigned char* nullIndicatorArray = getNullIndicatorArray();
  int byteNumber = index / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - index % 8));
  delete[] nullIndicatorArray;
  return (isNull);
}

unsigned char* RawRecord::getNullIndicatorArray() const {
  int size = getNullIndicatorSize();
  unsigned char* nullIndicatorArray = new unsigned char[size]();
  memcpy(&nullIndicatorArray, rawRecord, size);
  return (nullIndicatorArray);
}

int RawRecord::getNullIndicatorSize() const {
  int size = getSizeForNullIndicator(attributes.size());
  return (size);
}

size_t RawRecord::getRecordSize() const {
  size_t offset = getNullIndicatorSize();
  for (size_t i = 0; i < attributes.size(); ++i) {
    switch (attributes[i].type) {
      case TypeVarChar:
        int length;
        memcpy(&length, this->rawRecord + offset, sizeof(int));
        offset += sizeof(int) + length;
        break;
      default:  // for TypeInt and TypeReal
        offset += sizeof(int);
    }
  }
  return (offset);
}

Project::Project(Iterator* input, const vector<string>& attrNames) {
  this->input = input;
  attributeNames = attrNames;
}

Project::~Project() {}

RC Project::getNextTuple(void* data) {
  char* unprojectedData = new char[PAGE_SIZE]();
  RC isEOF = getNextTuple(unprojectedData);
  if (isEOF != 0) return (isEOF);

  vector<Attribute> attrs;
  input->getAttributes(attrs);
  RawRecord dataRecord(unprojectedData, attrs);

  //	getSizeForNullIndicator((int)(this->attributeNames.size()));
  //	int niaSize = ceil(this->attributeNames.size()/8.0);
  int niaSize = getSizeForNullIndicator((int)(this->attributeNames.size()));
  int offset = niaSize;
  unsigned char* nullIndicator = new unsigned char[niaSize]();
  for (size_t i = 0; i < attributeNames.size(); i++) {
    Value value = dataRecord.getAttributeValue(attributeNames[i]);
    if (value.data == nullptr) {
      makeFieldNull(nullIndicator, i);
      continue;
    }
    switch (value.type) {
      case TypeVarChar:
        int length;
        memcpy(&length, value.data, sizeof(int));
        memcpy((char*)data + offset, value.data, sizeof(int) + length);
        offset += sizeof(int) + length;
        break;
      default:
        memcpy((char*)data + offset, value.data, sizeof(int));
        offset += sizeof(int);
        break;
    }
  }
  return (isEOF);
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute>& attrs) const {
  vector<Attribute> unprojectedAttrs;
  input->getAttributes(unprojectedAttrs);

  for (string attrName : attributeNames) {
    for (Attribute attr : unprojectedAttrs) {
      if (attrName.compare(attr.name) == 0) attrs.push_back(attr);
    }
  }
}

BNLJoin::BNLJoin(
    Iterator* leftIn,            // Iterator of input R
    TableScan* rightIn,          // TableScan Iterator of input S
    const Condition& condition,  // Join condition
    const unsigned numPages      // # of pages that can be loaded into memory,
    //   i.e., memory block size (decided by the optimizer)
) {
  this->leftIn = leftIn;
  this->rightIn = rightIn;
  this->condition = condition;
  this->numPages = numPages;
  this->leftTableBufferOffset = 0;
  this->leftTableBuffer = new char[numPages * PAGE_SIZE]();
  this->rightTupleBuffer = new char[PAGE_SIZE]();
  // set attributes. call getAttributes only after setting left,right attrs
  this->leftIn->getAttributes(this->leftInAttributes);
  this->rightIn->getAttributes(this->rightInAttributes);
  this->getAttributes(joinedAttributes);
  // setup condition Evaluator
  this->cEval = new ConditionEvaluator(this->condition, joinedAttributes);
};

BNLJoin::~BNLJoin() {
  if (leftTableBuffer) {
    delete[] leftTableBuffer;
    leftTableBuffer = nullptr;
  }
  if (cEval) {
    delete cEval;
    cEval = nullptr;
  }
}

void BNLJoin::getAttributes(vector<Attribute>& attrs) const {
  // concatenation of left and right attributes
  attrs.insert(attrs.begin(), leftInAttributes.begin(), leftInAttributes.end());
  attrs.insert(attrs.end(), rightInAttributes.begin(), rightInAttributes.end());
}

/**
 *
 * @param leftTupleBuf : set the pointer to the next buffer offset if the offset
 * is valid. Else set it to nullptr.
 * @return false if you've reached the end of left buf, true otherwise
 */
bool BNLJoin::getNextLeftRecord(char const* leftTupleBuf) {
  if (leftTableBufferOffset >= this->sizeOfLeftBuffer) {
    leftTupleBuf = nullptr;
    return (false);
  }

  RawRecord currentRecord(leftTableBuffer + leftTableBufferOffset,
                          leftInAttributes);
  leftTupleBuf = leftTableBuffer + leftTableBufferOffset;
  leftTableBufferOffset += currentRecord.getRecordSize();
  return (true);
}

void BNLJoin::resetLeftOffset() { leftTableBufferOffset = 0; }

RC BNLJoin::setState() {
  char* leftTuple = nullptr;
  if (this->getNextLeftRecord(leftTuple)) {
    // there is a next record to read from left in-memory buffer
    // here you will use the existing rightTupleBuffer
    // and the next left tuple
    leftTuplePointer = leftTuple;

  } else {  //  there is no more left-records to read
    // here you will read a new rightTupleBuffer
    if (rightIn->getNextTuple(rightTupleBuffer) != QE_EOF) {
      // if there are more right tuples to read
      resetLeftOffset();  // start reading left table from start
      getNextLeftRecord(leftTuple);
      leftTuplePointer = leftTuple;
    } else {
      // if there are no more right records to read
      bool areThereLeftBlocksToRead = loadNextBlockInMemory();
      if (areThereLeftBlocksToRead) {
        // if there are more left blocks to read
        rightIn->setIterator();
        rightIn->getNextTuple(rightTupleBuffer);
      } else {
        // if there are no left blocks to read // everything has been read
        return QE_EOF;  // here we return failure;
      }
    }
  }
  return (success);
}

/**
 *
 * @param data : buffer to fill with tuple that satisfies the condition
 * @return 0 if the data points to a valid tuple, return -1 if no more records
 * satisfy the condition
 */
RC BNLJoin::getNextTuple(void* data) {
  // create a joined record buffer

  RC status = setState();

  const size_t MAX_JOINED_RECORD_SIZE = PAGE_SIZE * 2;
  char* joinedRecBuffer = new char[MAX_JOINED_RECORD_SIZE]();

  // get schema for joined record
  vector<Attribute> joinedAttrs;
  getAttributes(joinedAttrs);

  if (status == QE_EOF) return QE_EOF;

  // create raw record object to get record sizes
  RawRecord leftRecord(leftTuplePointer, leftInAttributes);
  RawRecord rightRecord(rightTupleBuffer, rightInAttributes);

  // get record sizes for left and right records
  size_t sizeOfLeftRecord = leftRecord.getRecordSize();
  size_t sizeOfRightRecord = rightRecord.getRecordSize();

  // prepare a joined record buffer by copying left and right records
  this->joinRecords(leftRecord, rightRecord, joinedRecBuffer);
  RawRecord joinedRecord(joinedRecBuffer, joinedAttrs);

  // Evaluate the condition on this join
  if (cEval->evaluateFor(joinedRecord)) {
    // if the join satisfies the condition then copy the joined record
    // to data buffer and return status success
    memcpy(data, joinedRecord.getBuffer(), joinedRecord.getRecordSize());
  }
  delete[] joinedRecBuffer;
  joinedRecBuffer = nullptr;

  return (success);
}

void BNLJoin::joinRecords(const RawRecord& leftRec, const RawRecord& rightRec,
                          char* joinedRecBuf) {
  vector<Attribute> joinedAttrs;
  this->getAttributes(joinedAttrs);

  int leftNIASize = leftRec.getNullIndicatorSize();
  int leftPayloadSize = leftRec.getRecordSize() - leftNIASize;
  unsigned char* leftNIA = leftRec.getNullIndicatorArray();

  int rightNIASize = rightRec.getNullIndicatorSize();
  int rightPayloadSize = rightRec.getRecordSize() - rightNIASize;
  unsigned char* rightNIA = rightRec.getNullIndicatorArray();

  int joinedNIASize = getSizeForNullIndicator((int)(joinedAttrs.size()));
  unsigned char* joinedNIA = new unsigned char[joinedNIASize]();

  for (size_t i = 0; i < joinedAttrs.size(); ++i) {
    if (i < leftRec.getAttributes().size()) {
      joinedNIA[i] = leftNIA[i];
    } else {
      int rightNIAIndex = i - leftRec.getAttributes().size();
      joinedNIA[i] = rightNIA[rightNIAIndex];
    }
  }

  // copy the joinedNIA
  memcpy(joinedRecBuf, joinedNIA, joinedNIASize);
  int joinedBufOffset = joinedNIASize;

  // copy the left payload
  memcpy(joinedRecBuf + joinedBufOffset, leftRec.getBuffer() + leftNIASize,
         leftPayloadSize);
  joinedBufOffset += leftPayloadSize;

  // copy the right payload
  memcpy(joinedRecBuf + joinedBufOffset, rightRec.getBuffer() + rightNIASize,
         rightPayloadSize);
}

RC BNLJoin::loadNextBlockInMemory() {
  memset(leftTableBuffer, 0, PAGE_SIZE * numPages);
  size_t lTblBuffOffset = 0;
  RC Left_EOF = 0;
  char* leftRecordBuffer = new char[PAGE_SIZE]();
  while (lTblBuffOffset < numPages * PAGE_SIZE) {
    Left_EOF = leftIn->getNextTuple(leftRecordBuffer);
    if (Left_EOF == QE_EOF) break;
    RawRecord leftRecord(leftRecordBuffer, leftInAttributes);
    size_t sizeOfRecord = leftRecord.getRecordSize();
    memcpy(this->leftTableBuffer + lTblBuffOffset, leftRecordBuffer,
           sizeOfRecord);
    lTblBuffOffset += sizeOfRecord;
  }
  sizeOfLeftBuffer = lTblBuffOffset;  // set the size of the left buffer
  leftTableBufferOffset = 0;          // reset the left table buffer offset
  delete[] leftRecordBuffer;
  if (Left_EOF == QE_EOF && lTblBuffOffset == 0)  // we do not want to send
  //	EOF if there are new records read into the left memory buffer
  {
    return QE_EOF;
  } else {
    return (success);
  }
}
