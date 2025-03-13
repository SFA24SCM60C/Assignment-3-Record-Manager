#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

// Initialize Storage Manager
void initStorageManager() {
    // Nothing to initialize in this basic implementation
}

// Create a new page file
RC createPageFile(char *fileName) {
    FILE *file = fopen(fileName, "w");
    if (!file) return RC_FILE_NOT_FOUND;
    
    char emptyPage[PAGE_SIZE] = {0};
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    fclose(file);
    
    return RC_OK;
}

// Open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "r+");
    if (!file) return RC_FILE_NOT_FOUND;
    
    fseek(file, 0, SEEK_END);
    int totalPages = ftell(file) / PAGE_SIZE;
    
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = totalPages;
    fHandle->mgmtInfo = file;
    
    return RC_OK;
}

// Close an open page file
RC closePageFile(SM_FileHandle *fHandle) {
    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (file) fclose(file);
    return RC_OK;
}

// Destroy (delete) a page file
RC destroyPageFile(char *fileName) {
    if (remove(fileName) == 0) return RC_OK;
    return RC_FILE_NOT_FOUND;
}

// Read a page from the file
RC readBlock(int pageNum, SM_FileHandle *fHandle, char *memPage) {
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) return RC_READ_NON_EXISTING_PAGE;
    
    FILE *file = (FILE *)fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
    fread(memPage, sizeof(char), PAGE_SIZE, file);
    
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

// Write a page to the file
RC writeBlock(int pageNum, SM_FileHandle *fHandle, char *memPage) {
    if (pageNum < 0) return RC_WRITE_FAILED;
    
    FILE *file = (FILE *)fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(memPage, sizeof(char), PAGE_SIZE, file);
    
    return RC_OK;
}

// Append a new empty block
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *file = (FILE *)fHandle->mgmtInfo;
    char emptyPage[PAGE_SIZE] = {0};
    
    fseek(file, 0, SEEK_END);
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    fHandle->totalNumPages++;
    
    return RC_OK;
}

// Ensure the file has a certain number of pages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    while (fHandle->totalNumPages < numberOfPages) {
        appendEmptyBlock(fHandle);
    }
    return RC_OK;
}
