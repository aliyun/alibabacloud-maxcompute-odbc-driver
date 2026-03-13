#ifndef MAXCOMPUTE_ODBC_ENTRY_POINTS_H
#define MAXCOMPUTE_ODBC_ENTRY_POINTS_H

#include "maxcompute_odbc/platform.h"  // Must include before sql.h on Windows
#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

// Function declarations for ODBC API entry points

// Handle Management Functions
SQLRETURN SQL_API SQLAllocEnv(SQLHENV* EnvironmentHandle);
SQLRETURN SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle,
                                  SQLHDBC* ConnectionHandle);
SQLRETURN SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle,
                               SQLHSTMT* StatementHandle);
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle,
                                 SQLHANDLE* OutputHandle);
SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle);
SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle);
SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option);
SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle);

// Connection Functions
SQLRETURN SQL_API SQLConnect(SQLHDBC ConnectionHandle, SQLCHAR* ServerName,
                             SQLSMALLINT NameLength1, SQLCHAR* UserName,
                             SQLSMALLINT NameLength2, SQLCHAR* Authentication,
                             SQLSMALLINT NameLength3);
SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC hDbc, SQLHWND hwndParent, SQLCHAR* szConnStrIn,
    SQLSMALLINT cbConnStrIn, SQLCHAR* szConnStrOut, SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT* pcbConnStrOut, SQLUSMALLINT fDriverCompletion);
SQLRETURN SQL_API SQLDriverConnectA(
    SQLHDBC hDbc, SQLHWND hwndParent, SQLCHAR* szConnStrIn,
    SQLSMALLINT cbConnStrIn, SQLCHAR* szConnStrOut, SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT* pcbConnStrOut, SQLUSMALLINT fDriverCompletion);
SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC hDbc, SQLHWND hwndParent,
                                    SQLWCHAR* szConnStrIn,
                                    SQLSMALLINT cbConnStrIn,
                                    SQLWCHAR* szConnStrOut,
                                    SQLSMALLINT cbConnStrOutMax,
                                    SQLSMALLINT* pcbConnStrOut,
                                    SQLUSMALLINT fDriverCompletion);
SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle);

// Statement Functions
SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                SQLCHAR* StatementText, SQLINTEGER TextLength);
SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR* StatementText,
                             SQLINTEGER TextLength);
SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle);
SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle);

// Column and Data Functions
SQLRETURN SQL_API SQLBindCol(SQLHSTMT StatementHandle,
                             SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                             SQLPOINTER TargetValue, SQLLEN BufferLength,
                             SQLLEN* StringLength);
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT StatementHandle,
                                   SQLSMALLINT* ColumnCount);
SQLRETURN SQL_API SQLDescribeCol(
    SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR* ColumnName,
    SQLSMALLINT BufferLength, SQLSMALLINT* NameLength, SQLSMALLINT* DataType,
    SQLULEN* ColumnSize, SQLSMALLINT* DecimalDigits, SQLSMALLINT* Nullable);
SQLRETURN SQL_API SQLRowCount(SQLHSTMT StatementHandle, SQLLEN* RowCount);
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber,
                                  SQLUSMALLINT FieldIdentifier,
                                  SQLPOINTER CharacterAttribute,
                                  SQLSMALLINT BufferLength,
                                  SQLSMALLINT* StringLength,
                                  SQLLEN* NumericAttribute);
SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT ColumnNumber,
                                   SQLUSMALLINT FieldIdentifier,
                                   SQLPOINTER CharacterAttribute,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT* StringLength,
                                   SQLLEN* NumericAttribute);
SQLRETURN SQL_API SQLGetData(SQLHSTMT StatementHandle,
                             SQLUSMALLINT ColOrParamNumber,
                             SQLSMALLINT TargetType, SQLPOINTER TargetValue,
                             SQLLEN BufferLength, SQLLEN* StringLength);

// Schema Functions
SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle, SQLCHAR* CatalogName,
                             SQLSMALLINT NameLength1, SQLCHAR* SchemaName,
                             SQLSMALLINT NameLength2, SQLCHAR* TableName,
                             SQLSMALLINT NameLength3, SQLCHAR* ColumnName,
                             SQLSMALLINT NameLength4);
SQLRETURN SQL_API SQLColumnsW(SQLHSTMT StatementHandle, SQLWCHAR* CatalogName,
                              SQLSMALLINT NameLength1, SQLWCHAR* SchemaName,
                              SQLSMALLINT NameLength2, SQLWCHAR* TableName,
                              SQLSMALLINT NameLength3, SQLWCHAR* ColumnName,
                              SQLSMALLINT NameLength4);
SQLRETURN SQL_API SQLTables(SQLHSTMT StatementHandle, SQLCHAR* CatalogName,
                            SQLSMALLINT NameLength1, SQLCHAR* SchemaName,
                            SQLSMALLINT NameLength2, SQLCHAR* TableName,
                            SQLSMALLINT NameLength3, SQLCHAR* TableType,
                            SQLSMALLINT NameLength4);
SQLRETURN SQL_API SQLTablesW(SQLHSTMT StatementHandle, SQLWCHAR* CatalogName,
                             SQLSMALLINT NameLength1, SQLWCHAR* SchemaName,
                             SQLSMALLINT NameLength2, SQLWCHAR* TableName,
                             SQLSMALLINT NameLength3, SQLWCHAR* TableType,
                             SQLSMALLINT NameLength4);

// Diagnostic Functions
SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                SQLSMALLINT RecNumber, SQLCHAR* Sqlstate,
                                SQLINTEGER* NativeError, SQLCHAR* MessageText,
                                SQLSMALLINT BufferLength,
                                SQLSMALLINT* TextLength);
SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                 SQLSMALLINT RecNumber, SQLWCHAR* Sqlstate,
                                 SQLINTEGER* NativeError, SQLWCHAR* MessageText,
                                 SQLSMALLINT BufferLength,
                                 SQLSMALLINT* TextLength);
SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                  SQLSMALLINT RecNumber,
                                  SQLSMALLINT FieldIdentifier,
                                  SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
                                  SQLSMALLINT* StringLength);
SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber,
                                   SQLSMALLINT FieldIdentifier,
                                   SQLPOINTER DiagInfo,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT* StringLength);

// Environment and Statement Attribute Functions
SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
                                SQLPOINTER Value, SQLINTEGER StringLength);
SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                 SQLPOINTER Value, SQLINTEGER BufferLength,
                                 SQLINTEGER* StringLength);
SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                 SQLPOINTER ValuePtr, SQLINTEGER StringLength);
SQLRETURN SQL_API SQLSetStmtAttrW(SQLHSTMT StatementHandle,
                                  SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                  SQLINTEGER StringLength);

// Information Functions
SQLRETURN SQL_API SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                             SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                             SQLSMALLINT* StringLengthPtr);
SQLRETURN SQL_API SQLGetInfoW(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                              SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                              SQLSMALLINT* StringLengthPtr);

// Connection Attribute Functions
SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                    SQLINTEGER BufferLength,
                                    SQLINTEGER* StringLengthPtr);
SQLRETURN SQL_API SQLGetConnectAttrW(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                     SQLINTEGER BufferLength,
                                     SQLINTEGER* StringLengthPtr);
SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                    SQLINTEGER StringLength);
SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                                     SQLINTEGER StringLength);

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT StatementHandle);

#endif  // MAXCOMPUTE_ODBC_ENTRY_POINTS_H