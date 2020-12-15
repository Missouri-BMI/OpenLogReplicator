/* Class to hold RowId data
   Copyright (C) 2018-2020 Adam Leszczynski (aleszczynski@bersler.com)

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

#include "RowId.h"

using namespace std;

namespace OpenLogReplicator {

    const char RowId::map64[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const char RowId::map64R[256] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 0, 0, 0, 63,
            52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0, 0, 0, 0, 0, 0,
            0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 0, 0, 0, 0, 0,
            0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    RowId::RowId() : dataObj(0), dba(0), slot(0) {
    }

    RowId::RowId(const char *rowid) {
        if (strlen(rowid) != 18) {
            ERROR("RowID: incorrect format: " << rowid);
        }
        dataObj = (((typeDBA)map64R[rowid[0]]) << 30) |
                (((typeDBA)map64R[rowid[1]]) << 24) |
                (((typeDBA)map64R[rowid[2]]) << 18) |
                (((typeDBA)map64R[rowid[3]]) << 12) |
                (((typeDBA)map64R[rowid[4]]) << 6) |
                ((typeDBA)map64R[rowid[5]]);

        typeAFN afn = (((typeAFN)map64R[rowid[6]]) << 12) |
                (((typeAFN)map64R[rowid[7]]) << 6) |
                ((typeAFN)map64R[rowid[8]]);

        dba = (((typeDBA)map64R[rowid[9]]) << 30) |
                (((typeDBA)map64R[rowid[10]]) << 24) |
                (((typeDBA)map64R[rowid[1]]) << 18) |
                (((typeDBA)map64R[rowid[12]]) << 12) |
                (((typeDBA)map64R[rowid[13]]) << 6) |
                ((typeDBA)map64R[rowid[14]]) |
                (((typeDBA)afn) << 22);

        slot = (((typeSLOT)map64R[rowid[15]]) << 12) |
                (((typeSLOT)map64R[rowid[16]]) << 6) |
                ((typeSLOT)map64R[rowid[17]]);
    }

    bool RowId::operator<(const RowId& other) const {
        if (other.dataObj < dataObj)
            return true;
        if (other.dba < dba)
            return true;
        if (other.slot < slot)
            return true;
        return false;
    }

    bool RowId::operator==(const RowId& other) const {
        return (other.dataObj == dataObj) &&
                (other.dba == dba) &&
                (other.slot == slot);
    }

    bool RowId::operator!=(const RowId& other) const {
        return (other.dataObj != dataObj) ||
                (other.dba != dba) ||
                (other.slot != slot);
    }
}

namespace std {
    size_t std::hash<OpenLogReplicator::RowId>::operator()(const OpenLogReplicator::RowId &rowId) const {
        return hash<typeDATAOBJ>()(rowId.dataObj) ^
                hash<typeDBA>()(rowId.dba) ^
                hash<typeSLOT>()(rowId.slot);
    }
}

