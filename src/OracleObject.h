/* Header for OracleObject class
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

#include <string>
#include <vector>

#include "types.h"

#ifndef ORACLEOBJECT_H_
#define ORACLEOBJECT_H_

using namespace std;

namespace OpenLogReplicator {

    class OracleColumn;

    class OracleObject {
    public:
        typeobj objn;
        typeobj objd;
        uint64_t cluCols;
        uint64_t totalPk;
        uint64_t options;
        uint64_t maxSegCol;
        string owner;
        string objectName;
        vector<OracleColumn*> columns;
        vector<typeobj2> partitions;

        void addColumn(OracleColumn *column);
        void addPartition(typeobj partitionObjn, typeobj partitionObjd);

        OracleObject(typeobj objn, typeobj objd, uint64_t cluCols, uint64_t options, const char *owner, const char *objectName);
        virtual ~OracleObject();

        friend ostream& operator<<(ostream& os, const OracleObject& ors);
    };
}

#endif
