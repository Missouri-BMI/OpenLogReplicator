/* Definition of types and macros
   Copyright (C) 2018-2021 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string.h>
#include <time.h>

#include "../config.h"

#ifndef TYPES_H_
#define TYPES_H_

typedef uint16_t typesum;
typedef uint32_t typeresetlogs;
typedef uint32_t typeactivation;
typedef uint16_t typeop1;
typedef uint32_t typeop2;
typedef uint16_t typeconid;
typedef uint32_t typeblk;
typedef uint8_t typeslt;
typedef uint8_t typerci;

typedef uint64_t typeUBA;
typedef uint32_t typeSEQ;
typedef uint64_t typeSCN;
typedef uint16_t typeSubSCN;
typedef uint64_t typeXID;
typedef uint64_t typeXIDMAP;
typedef uint16_t typeAFN;
typedef uint32_t typeDBA;
typedef uint16_t typeSLOT;
typedef uint32_t typeOBJ;
typedef uint32_t typeDATAOBJ;
typedef uint64_t typeOBJ2;
typedef int16_t typeCOL;
typedef uint16_t typeTYPE;
typedef uint32_t typeCON;
typedef uint32_t typeUSER;

typedef uint16_t typeunicode16;
typedef uint32_t typeunicode32;
typedef uint64_t typeunicode;

#define ZERO_SCN                                ((typeSCN)0xFFFFFFFFFFFFFFFF)
#define MAX_PATH_LENGTH                         2048
#define MAX_FIELD_LENGTH                        1048576
#define MAX_NO_COLUMNS                          1000
#define MAX_TRANSACTIONS_LIMIT                  1048576
#define MAX_RECORDS_IN_LWN                      262144
#define MEMORY_CHUNK_SIZE_MB                    1
#define MEMORY_CHUNK_SIZE_MB_CHR                "1"
#define MEMORY_CHUNK_SIZE                       (MEMORY_CHUNK_SIZE_MB*1024*1024)
#define MEMORY_CHUNK_MIN_MB                     16
#define MEMORY_CHUNK_MIN_MB_CHR                 "16"

#define WRITER_KAFKA                            1
#define WRITER_FILE                             2
#define WRITER_SERVICE                          3

#define SAVEPOINT_OFF                           0
#define SAVEPOINT_FILES                         1

#define ARCH_LOG_PATH                           0
#define ARCH_LOG_ONLINE                         1
#define ARCH_LOG_ONLINE_KEEP                    2
#define ARCH_LOG_LIST                           3

#define MESSAGE_FORMAT_SHORT                    0
#define MESSAGE_FORMAT_FULL                     1

#define TIMESTAMP_FORMAT_UNIX                   0
#define TIMESTAMP_FORMAT_ISO8601                1
#define TIMESTAMP_FORMAT_ALL_PAYLOADS           2

#define CHAR_FORMAT_UTF8                        0
#define CHAR_FORMAT_NOMAPPING                   1
#define CHAR_FORMAT_HEX                         2

#define SCN_FORMAT_NUMERIC                      0
#define SCN_FORMAT_HEX                          1
#define SCN_FORMAT_ALL_PAYLOADS                 2

#define XID_FORMAT_TEXT                         0
#define XID_FORMAT_NUMERIC                      1

#define UNKNOWN_FORMAT_QUESTION                 0
#define UNKNOWN_FORMAT_DUMP                     1

#define SCHEMA_FORMAT_NAME                      0
#define SCHEMA_FORMAT_FULL                      1
#define SCHEMA_FORMAT_REPEATED                  2
#define SCHEMA_FORMAT_OBJ                       4

//default, only changed columns for update, or PK
#define COLUMN_FORMAT_CHANGED                   0
//show full nulls from insert & delete
#define COLUMN_FORMAT_INS_DEC                   1
//show all from redo
#define COLUMN_FORMAT_FULL                      2

#define TRACE_SILENT                            0
#define TRACE_WARNING                           1
#define TRACE_INFO                              2
#define TRACE_FULL                              3

#define TRACE2_DML                              0x0000001
#define TRACE2_DUMP                             0x0000002
#define TRACE2_LWN                              0x0000004
#define TRACE2_THREADS                          0x0000008
#define TRACE2_SQL                              0x0000010
#define TRACE2_FILE                             0x0000020
#define TRACE2_DISK                             0x0000040
#define TRACE2_MEMORY                           0x0000080
#define TRACE2_PERFORMANCE                      0x0000100
#define TRACE2_TRANSACTION                      0x0000200
#define TRACE2_REDO                             0x0000400
#define TRACE2_ARCHIVE_LIST                     0x0000800
#define TRACE2_SCHEMA_LIST                      0x0001000
#define TRACE2_KAFKA                            0x0002000

#define REDO_FLAGS_ARCH_ONLY                    0x0000001
#define REDO_FLAGS_SCHEMALESS                   0x0000002
#define REDO_FLAGS_DIRECT                       0x0000004
#define REDO_FLAGS_NOATIME                      0x0000008
#define REDO_FLAGS_ON_ERROR_CONTINUE            0x0000010
#define REDO_FLAGS_TRACK_DDL                    0x0000020
#define REDO_FLAGS_SHOW_INVISIBLE_COLUMNS       0x0000040
#define REDO_FLAGS_SHOW_CONSTRAINT_COLUMNS      0x0000080
#define REDO_FLAGS_SHOW_INCOMPLETE_TRANSACTIONS 0x0000100
#define REDO_FLAGS_FLUSH_QUEUE_ON_EXIT          0x0000200
#define REDO_FLAGS_SAVEPOINTS_OFF               0x0000400

#define DISABLE_CHECK_GRANTS                    0x0000001
#define DISABLE_CHECK_SUPPLEMENTAL_LOG          0x0000002
#define DISABLE_CHECK_BLOCK_SUM                 0x0000004

#define TRANSACTION_INSERT                      1
#define TRANSACTION_DELETE                      2
#define TRANSACTION_UPDATE                      3

#define OUTPUT_BUFFER_DATA_SIZE                 (MEMORY_CHUNK_SIZE - sizeof(struct OutputBufferQueue))
#define OUTPUT_BUFFER_ALLOCATED                 0x0001
#define OUTPUT_BUFFER_CONFIRMED                 0x0002

#define VALUE_BEFORE                            0
#define VALUE_AFTER                             1
#define VALUE_BEFORE_SUPP                       2
#define VALUE_AFTER_SUPP                        3

#define USN(xid)                                ((uint16_t)(((uint64_t)xid)>>48))
#define SLT(xid)                                ((uint16_t)(((((uint64_t)xid)>>32)&0xFFFF)))
#define SQN(xid)                                ((uint32_t)(((xid)&0xFFFFFFFF)))
#define XID(usn,slt,sqn)                        ((((uint64_t)(usn))<<48)|(((uint64_t)(slt))<<32)|((uint64_t)(sqn)))
#define PRINTXID(xid)                           "0x"<<setfill('0')<<setw(4)<<hex<<USN(xid)<<"."<<setw(3)<<SLT(xid)<<"."<<setw(8)<<SQN(xid)

#define BLOCK(uba)                              ((uint32_t)((uba)&0xFFFFFFFF))
#define SEQUENCE(uba)                           ((uint16_t)((((uint64_t)uba)>>32)&0xFFFF))
#define RECORD(uba)                             ((uint8_t)((((uint64_t)uba)>>48)&0xFF))
#define PRINTUBA(uba)                           "0x"<<setfill('0')<<setw(8)<<hex<<BLOCK(uba)<<"."<<setfill('0')<<setw(4)<<hex<<SEQUENCE(uba)<<"."<<setfill('0')<<setw(2)<<hex<<(uint32_t)RECORD(uba)

#define SCN(scn1,scn2)                          ((((uint64_t)scn1)<<32)|(scn2))
#define PRINTSCN48(scn)                         "0x"<<setfill('0')<<setw(4)<<hex<<((uint32_t)((scn)>>32)&0xFFFF)<<"."<<setw(8)<<((scn)&0xFFFFFFFF)
#define PRINTSCN64(scn)                         "0x"<<setfill('0')<<setw(16)<<hex<<(scn)

#define DUMP(x)                                 {stringstream s; s << x << endl; cerr << s.str(); }
#define ERROR(x)                                {stringstream s; s << "ERROR: " << x << endl; cerr << s.str(); }
#define OUT(x)                                  {cerr << x; }

#define WARNING(x)                              {if (oracleAnalyzer->trace >= TRACE_INFO){stringstream s; s << "WARNING: " << x << endl; cerr << s.str();} }
#define INFO(x)                                 {if (oracleAnalyzer->trace >= TRACE_INFO){stringstream s; s << "INFO: " << x << endl; cerr << s.str();} }
#define FULL(x)                                 {if (oracleAnalyzer->trace >= TRACE_FULL){stringstream s; s << "FULL: " << x << endl; cerr << s.str();} }
#define TRACE(t,x)                              {if ((oracleAnalyzer->trace2 & (t)) != 0) {stringstream s; s << "TRACE: " << x << endl; cerr << s.str();} }

#define WARNING_(x)                             {if (trace >= TRACE_INFO){stringstream s; s << "WARNING: " << x << endl; cerr << s.str();} }
#define INFO_(x)                                {if (trace >= TRACE_INFO){stringstream s; s << "INFO: " << x << endl; cerr << s.str();} }
#define FULL_(x)                                {if (trace >= TRACE_FULL){stringstream s; s << "FULL: " << x << endl; cerr << s.str();} }
#define TRACE_(t,x)                             {if ((trace2 & (t)) != 0) {stringstream s; s << "TRACE: " << x << endl; cerr << s.str();} }
#define TYPEINTXLEN                             4

using namespace std;

namespace OpenLogReplicator {

    class OracleAnalyzer;

    class uintX_t {
    private:
        uint64_t data[TYPEINTXLEN];
    public:
        uintX_t& operator+=(const uintX_t &val);
        uintX_t& operator=(const uintX_t &val);
        uintX_t& operator=(uint64_t val);
        uintX_t& operator=(const char *val);
        uintX_t& set(uint64_t val1, uint64_t val2);

        friend ostream& operator<<(ostream& os, const uintX_t& val);
    };

    class typetime {
        uint32_t val;
    public:
        typetime(void) :
            val(0) {
        }

        typetime(uint32_t val) :
            val(val) {
        }

        uint32_t getVal(void) {
            return this->val;
        }

        typetime& operator= (uint32_t val) {
            this->val = val;
            return *this;
        }

        time_t toTime(void) {
            struct tm epochtime = {0};
            memset((void*)&epochtime, 0, sizeof(epochtime));
            uint64_t rest = val;
            epochtime.tm_sec = rest % 60; rest /= 60;
            epochtime.tm_min = rest % 60; rest /= 60;
            epochtime.tm_hour = rest % 24; rest /= 24;
            epochtime.tm_mday = (rest % 31) + 1; rest /= 31;
            epochtime.tm_mon = (rest % 12); rest /= 12;
            epochtime.tm_year = rest + 88;
            return mktime(&epochtime);
        }

        void toISO8601(char *buffer) {
            uint64_t rest = val;
            uint64_t ss = rest % 60; rest /= 60;
            uint64_t mi = rest % 60; rest /= 60;
            uint64_t hh = rest % 24; rest /= 24;
            uint64_t dd = (rest % 31) + 1; rest /= 31;
            uint64_t mm = (rest % 12) + 1; rest /= 12;
            uint64_t yy = rest + 1988;
            buffer[3] = '0' + (yy % 10); yy /= 10;
            buffer[2] = '0' + (yy % 10); yy /= 10;
            buffer[1] = '0' + (yy % 10); yy /= 10;
            buffer[0] = '0' + yy;
            buffer[4] = '-';
            buffer[6] = '0' + (mm % 10); mm /= 10;
            buffer[5] = '0' + mm;
            buffer[7] = '-';
            buffer[9] = '0' + (dd % 10); dd /= 10;
            buffer[8] = '0' + dd;
            buffer[10] = 'T';
            buffer[12] = '0' + (hh % 10); hh /= 10;
            buffer[11] = '0' + hh;
            buffer[13] = ':';
            buffer[15] = '0' + (mi % 10); mi /= 10;
            buffer[14] = '0' + mi;
            buffer[16] = ':';
            buffer[18] = '0' + (ss % 10); ss /= 10;
            buffer[17] = '0' + ss;
            buffer[19] = 'Z';
            buffer[20] = 0;
            //01234567890123456789
            //YYYY-MM-DDThh:mm:ssZ
        }

        friend ostream& operator<<(ostream& os, const typetime& time) {
            uint64_t rest = time.val;
            uint64_t ss = rest % 60; rest /= 60;
            uint64_t mi = rest % 60; rest /= 60;
            uint64_t hh = rest % 24; rest /= 24;
            uint64_t dd = (rest % 31) + 1; rest /= 31;
            uint64_t mm = (rest % 12) + 1; rest /= 12;
            uint64_t yy = rest + 1988;
            os << setfill('0') << setw(2) << dec << mm << "/" << setfill('0') << setw(2) << dec << dd << "/" << yy << " " <<
                    setfill('0') << setw(2) << dec << hh << ":" << setfill('0') << setw(2) << dec << mi << ":" << setfill('0') << setw(2) << dec << ss;
            return os;
            //0123456789012345678
            //DDDDDDDDDD HHHHHHHH
            //10/15/2018 22:25:36
        }
    };
}

#define CHECKPOINT_SIZE 12

#endif
