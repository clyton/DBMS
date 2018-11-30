
#include "qe.h"
#include "../rbf/rbfm.h"

#include <stddef.h>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>


Filter::Filter(Iterator* input, const Condition &condition) {

}


RawRecord::RawRecord(char* rawRecord, vector<Attribute> &attrs){
	this->rawRecord = rawRecord;
	this->attributes = attrs;
	setUpAttributeValue();
}
// ... the rest of your implementations go here


void RawRecord::setUpAttributeValue(){
	int sizeOfNullIndicatorArray = ceil(attributes.size()/8.0);
	unsigned char* nullIndicatorArray = new unsigned char[sizeOfNullIndicatorArray]();
	memcpy(&nullIndicatorArray, rawRecord, sizeOfNullIndicatorArray);
	int offset = sizeOfNullIndicatorArray;
	for (size_t i = 0; i < attributes.size(); ++i) {

		Value value;
		value.type = attributes[i].type;
		if(isFieldNull(nullIndicatorArray, (int)i)){
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

ConditionEvaluator::ConditionEvaluator(Condition& condition,vector<Attribute> &attrs){
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

