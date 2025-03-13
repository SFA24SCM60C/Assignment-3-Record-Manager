#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

// Frame structure for pages in memory
typedef struct Frame {
    PageNumber pageNum;
    char *data;
    bool dirty;
    int fixCount;
    struct Frame *next;
} Frame;

// Buffer Manager metadata
typedef struct BM_MgmtData {
    Frame *frames;
    int numPages;
    int readCount;
    int writeCount;
} BM_MgmtData;

// Initialize the buffer pool
RC initBufferPool(BM_BufferPool *bp, const char *pageFileName, int numPages, ReplacementStrategy strategy, void *stratData) {
    bp->pageFile = (char *)malloc(strlen(pageFileName) + 1);
    strcpy(bp->pageFile, pageFileName);
    bp->numPages = numPages;
    bp->strategy = strategy;
    
    BM_MgmtData *mgmtData = (BM_MgmtData *)malloc(sizeof(BM_MgmtData));
    mgmtData->frames = (Frame *)calloc(numPages, sizeof(Frame));
    mgmtData->numPages = numPages;
    mgmtData->readCount = 0;
    mgmtData->writeCount = 0;
    
    for (int i = 0; i < numPages; i++) {
        mgmtData->frames[i].pageNum = NO_PAGE;
        mgmtData->frames[i].data = (char *)malloc(PAGE_SIZE);
        mgmtData->frames[i].dirty = false;
        mgmtData->frames[i].fixCount = 0;
    }
    
    bp->mgmtData = mgmtData;
    return RC_OK;
}

// Shutdown buffer pool
RC shutdownBufferPool(BM_BufferPool *bp) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bp->mgmtData;
    
    for (int i = 0; i < mgmtData->numPages; i++) {
        free(mgmtData->frames[i].data);
    }
    free(mgmtData->frames);
    free(mgmtData);
    free(bp->pageFile);
    
    return RC_OK;
}

// Force flush dirty pages
RC forceFlushPool(BM_BufferPool *bp) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bp->mgmtData;
    for (int i = 0; i < mgmtData->numPages; i++) {
        if (mgmtData->frames[i].dirty && mgmtData->frames[i].fixCount == 0) {
            SM_FileHandle fh;
            openPageFile(bp->pageFile, &fh);
            writeBlock(mgmtData->frames[i].pageNum, &fh, mgmtData->frames[i].data);
            mgmtData->frames[i].dirty = false;
            closePageFile(&fh);
        }
    }
    return RC_OK;
}

// Pin a page into the buffer pool
RC pinPage(BM_BufferPool *bp, BM_PageHandle *ph, PageNumber pageNum) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bp->mgmtData;
    Frame *frame = NULL;
    
    for (int i = 0; i < mgmtData->numPages; i++) {
        if (mgmtData->frames[i].pageNum == pageNum) {
            frame = &mgmtData->frames[i];
            break;
        }
    }
    
    if (frame == NULL) {
        for (int i = 0; i < mgmtData->numPages; i++) {
            if (mgmtData->frames[i].fixCount == 0) {
                frame = &mgmtData->frames[i];
                break;
            }
        }
    }
    
    if (frame == NULL) return RC_BUFFER_POOL_FULL;
    
    SM_FileHandle fh;
    openPageFile(bp->pageFile, &fh);
    readBlock(pageNum, &fh, frame->data);
    closePageFile(&fh);
    
    frame->pageNum = pageNum;
    frame->fixCount++;
    
    ph->pageNum = pageNum;
    ph->data = frame->data;
    
    return RC_OK;
}

// Unpin a page from the buffer pool
RC unpinPage(BM_BufferPool *bp, BM_PageHandle *ph) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bp->mgmtData;
    for (int i = 0; i < mgmtData->numPages; i++) {
        if (mgmtData->frames[i].pageNum == ph->pageNum) {
            mgmtData->frames[i].fixCount--;
            return RC_OK;
        }
    }
    return RC_ERROR; // Added return statement
}

RC markDirty(BM_BufferPool *bp, BM_PageHandle *ph) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bp->mgmtData;
    for (int i = 0; i < mgmtData->numPages; i++) {
        if (mgmtData->frames[i].pageNum == ph->pageNum) {
            mgmtData->frames[i].dirty = true;
            return RC_OK;
        }
    }
    return RC_ERROR; // Added return statement
}

// Force write of a page to disk
RC forcePage(BM_BufferPool *bp, BM_PageHandle *ph) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bp->mgmtData;
    for (int i = 0; i < mgmtData->numPages; i++) {
        if (mgmtData->frames[i].pageNum == ph->pageNum) {
            SM_FileHandle fh;
            openPageFile(bp->pageFile, &fh);
            writeBlock(ph->pageNum, &fh, mgmtData->frames[i].data);
            closePageFile(&fh);
            mgmtData->frames[i].dirty = false;
            return RC_OK;
        }
    }
    return RC_ERROR;
}
