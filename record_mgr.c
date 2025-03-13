#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// Structure for managing table metadata
typedef struct RM_TableMgmtData {
    BM_BufferPool bufferPool;
    BM_PageHandle pageHandle;
    int numTuples;
    int freePage;
} RM_TableMgmtData;

// Structure for scan management
typedef struct RM_ScanMgmtData {
    int currentPage;
    int currentSlot;
    Expr *condition;
} RM_ScanMgmtData;

// Initialize the Record Manager
RC initRecordManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

// Shutdown the Record Manager
RC shutdownRecordManager() {
    return RC_OK;
}

// Create a new table
RC createTable(char *name, Schema *schema) {
    SM_FileHandle fileHandle;
    createPageFile(name);
    openPageFile(name, &fileHandle);
    
    // Allocate space for metadata
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) malloc(sizeof(RM_TableMgmtData));
    tableData->numTuples = 0;
    tableData->freePage = 1;
    
    char *pageData = (char *) calloc(PAGE_SIZE, sizeof(char));
    memcpy(pageData, &tableData->numTuples, sizeof(int));
    memcpy(pageData + sizeof(int), &tableData->freePage, sizeof(int));
    writeBlock(0, &fileHandle, pageData);
    
    free(pageData);
    closePageFile(&fileHandle);
    free(tableData);
    return RC_OK;
}

// Open an existing table
RC openTable(RM_TableData *rel, char *name) {
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) malloc(sizeof(RM_TableMgmtData));
    
    // Initialize buffer pool
    initBufferPool(&tableData->bufferPool, name, 3, RS_FIFO, NULL);
    
    // Read metadata
    pinPage(&tableData->bufferPool, &tableData->pageHandle, 0);
    char *pageData = tableData->pageHandle.data;
    memcpy(&tableData->numTuples, pageData, sizeof(int));
    memcpy(&tableData->freePage, pageData + sizeof(int), sizeof(int));
    
    rel->mgmtData = tableData;
    return RC_OK;
}

// Close a table
RC closeTable(RM_TableData *rel) {
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) rel->mgmtData;
    shutdownBufferPool(&tableData->bufferPool);
    free(tableData);
    return RC_OK;
}

// Delete a table
RC deleteTable(char *name) {
    destroyPageFile(name);
    return RC_OK;
}

// Get number of tuples
int getNumTuples(RM_TableData *rel) {
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) rel->mgmtData;
    return tableData->numTuples;
}

// Insert a record into a table
RC insertRecord(RM_TableData *rel, Record *record) {
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) rel->mgmtData;
    
    pinPage(&tableData->bufferPool, &tableData->pageHandle, tableData->freePage);
    char *pageData = tableData->pageHandle.data;
    
    int slot = tableData->numTuples % (PAGE_SIZE / getRecordSize(rel->schema));
    int page = tableData->numTuples / (PAGE_SIZE / getRecordSize(rel->schema));
    
    record->id.page = page;
    record->id.slot = slot;
    memcpy(pageData + (slot * getRecordSize(rel->schema)), record->data, getRecordSize(rel->schema));
    
    tableData->numTuples++;
    markDirty(&tableData->bufferPool, &tableData->pageHandle);
    unpinPage(&tableData->bufferPool, &tableData->pageHandle);
    return RC_OK;
}

// Delete a record from a table
RC deleteRecord(RM_TableData *rel, RID id) {
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) rel->mgmtData;
    pinPage(&tableData->bufferPool, &tableData->pageHandle, id.page);
    char *pageData = tableData->pageHandle.data;
    memset(pageData + (id.slot * getRecordSize(rel->schema)), 0, getRecordSize(rel->schema));
    markDirty(&tableData->bufferPool, &tableData->pageHandle);
    unpinPage(&tableData->bufferPool, &tableData->pageHandle);
    return RC_OK;
}

// Retrieve a record
RC getRecord(RM_TableData *rel, RID id, Record *record) {
    RM_TableMgmtData *tableData = (RM_TableMgmtData *) rel->mgmtData;
    pinPage(&tableData->bufferPool, &tableData->pageHandle, id.page);
    char *pageData = tableData->pageHandle.data;
    record->id = id;
    record->data = (char *) malloc(getRecordSize(rel->schema));
    memcpy(record->data, pageData + (id.slot * getRecordSize(rel->schema)), getRecordSize(rel->schema));
    unpinPage(&tableData->bufferPool, &tableData->pageHandle);
    return RC_OK;
}

// Free allocated memory for a record
RC freeRecord(Record *record) {
    if (record == NULL) return RC_ERROR;
    free(record->data);
    free(record);
    return RC_OK;
}

// Retrieve an attribute value from a record
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    if (attrNum < 0 || attrNum >= schema->numAttr) return RC_ERROR;
    *value = (Value *) malloc(sizeof(Value));
    (*value)->dt = schema->dataTypes[attrNum];
    (*value)->v.intV = *((int *)(record->data + attrNum * sizeof(int))); 
    return RC_OK;
}

// Set an attribute value in a record
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    if (attrNum < 0 || attrNum >= schema->numAttr) return RC_ERROR;
    memcpy(record->data + attrNum * sizeof(int), &value->v.intV, sizeof(int));
    return RC_OK;
}
