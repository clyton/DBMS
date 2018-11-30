
#include "qe.h"

#include <stddef.h>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>



Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
	getAttributes(attributes);
	this->cEval = new ConditionEvaluator(condition, attributes);
}

RC Filter::getNextTuple(void *data) {
	RC isEOF = input->getNextTuple(data);
	RawRecord dataRecord((char*)data, attributes);
	if (isEOF == 0){
		if (cEval->evaluateFor(dataRecord)){
			return ( isEOF );
		}
	}
	return ( isEOF );

}
void Filter::getAttributes(vector<Attribute> &attrs) const{
	input->getAttributes(attrs);
}

RawRecord::RawRecord(char* rawRecord, vector<Attribute> &attrs){
	this->rawRecord = rawRecord;
	this->attributes = attrs;
	setUpAttributeValue();
}
// ... the rest of your implementations go here


void RawRecord::setUpAttributeValue(){
	int size = getNullIndicatorSize();

	int offset = size;
	for (size_t i = 0; i < attributes.size(); ++i) {

		Value value;
		value.type = attributes[i].type;
		if(isFieldNull(i)){
			value.data = nullptr;
			attributeValue.push_back(value);
			continue;
		}


		// if the value is not null
		value.data = new char[attributes[i].length + 1]();

		switch(value.type){
		case TypeInt: memcpy(&value.data, rawRecord + offset, sizeof(int));
			offset += sizeof(int);
			break;
		case TypeReal: memcpy(&value.data, rawRecord + offset, sizeof(int));
			offset += sizeof(int);
			break;
		case TypeVarChar: int length;
			memcpy(&length, rawRecord+ offset, sizeof(int));
			memcpy(&value.data, rawRecord + offset, sizeof(length) + length);
			offset += sizeof(length) + length;
			break;
		}

	attributeValue.push_back(value);
	}

}

Value& RawRecord::getAttributeValue(int index){
	return (attributeValue[index]);
}

Value& RawRecord::getAttributeValue(Attribute& attribute){
	for (size_t i = 0; i < attributes.size(); ++i) {
		if(attributes[i].name.compare(attribute.name) == 0)
			return (getAttributeValue(i));
	}
	cerr << "Wrong attribute name";
	exit(1);
}

Value& RawRecord::getAttributeValue(string &attributeName){
	for (size_t i = 0; i < attributes.size(); ++i) {
		if(attributes[i].name.compare(attributeName) == 0)
			return (getAttributeValue(i));
	}
	cerr << "Wrong attribute name";
	exit(1);
}

ConditionEvaluator::ConditionEvaluator(const Condition& condition,vector<Attribute> &attrs){
	this->condition = condition;
	this->attributes = attrs;
}

bool ConditionEvaluator::evaluateFor(RawRecord& rawRecord){
	CompOp compOp = condition.op;
	char* value;
	Value lhsValue = rawRecord.getAttributeValue(condition.lhsAttr);

	if (condition.bRhsIsAttr){
		value = (char*) rawRecord.getAttributeValue(condition.rhsAttr).data;
	}
	else {
		value = (char*) condition.rhsValue.data;
	}
	return ( CheckCondition(lhsValue.type,(char*)lhsValue.data, value, compOp) );
}


bool RawRecord::isFieldNull(int index){
	unsigned char* nullIndicatorArray = getNullIndicatorArray();
	int byteNumber = index / 8;
	bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - index % 8));
	delete[] nullIndicatorArray;
	return ( isNull );
}

unsigned char* RawRecord::getNullIndicatorArray() {
	int size = getNullIndicatorSize();
	unsigned char* nullIndicatorArray = new unsigned char[size]();
	memcpy(&nullIndicatorArray, rawRecord, size);
	return ( nullIndicatorArray );
}

int RawRecord::getNullIndicatorSize(){
	int size = attributes.size()/8.0;
	return ( size );
}
