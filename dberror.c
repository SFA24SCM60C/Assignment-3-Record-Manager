#include "dberror.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

char *RC_message = "Unknown error";

/* Print a message to standard out describing the error */
void printError(RC error) {
    printf("%s\n", errorMessage(error));
}

/* Return an error message string */
char *errorMessage(RC error) {
    switch (error) {
        case RC_OK: return "Operation successful";
        case RC_FILE_NOT_FOUND: return "File not found";
        case RC_READ_NON_EXISTING_PAGE: return "Attempted to read a non-existing page";
        case RC_WRITE_FAILED: return "Write operation failed";
        case RC_BUFFER_POOL_FULL: return "Buffer pool is full";
        case RC_ERROR: return "General error occurred";
        default: return "Unknown error";
    }
}
