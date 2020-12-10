/* Header for Schema class
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

#include <unordered_map>
#include <vector>

#include "types.h"

#ifndef SCHEMA_H_
#define SCHEMA_H_

using namespace std;

namespace OpenLogReplicator {
    class OracleAnalyzer;
    class OracleObject;
    class SchemaElement;

    struct SysCCol {
        string rowid;
        typecol con;        //pk
        typecol intCol;     //pk
        uint64_t objn;
        uint64_t spare1;
    };

    struct SysCDef {
        string rowid;
        typecol con;        //pk
        typeobj objn;
        uint64_t type;
    };

    struct SysCol {
        string rowid;
        typeobj objn;       //pk
        typecol col;
        typecol segCol;
        typecol intCol;     //pk
        string name;
        uint64_t type;
        uint64_t length;
        int64_t precision;
        int64_t scale;
        uint64_t charsetForm;
        uint64_t charsetId;
        int64_t null;
        uintX_t property;
    };

    struct SysECol {
        string rowid;
        typeobj objn;
        uint32_t colNum;
        uint32_t guardId;
    };

    struct SysObj {
        string rowid;
        typeobj objn;       //pk
        typeobj objd;
        uint32_t owner;
        uint64_t type;
        string name;
        uint32_t flags;
    };

    struct SysUser {
        string rowid;
        uint64_t user;      //pk
        string name;
        uint64_t spare1;
    };

    struct SysTab {
        string rowid;
        typeobj objn;       //pk
        typeobj objd;
        uint32_t ts;
        uint32_t file;
        uint32_t block;
        uint64_t cluCols;
        uint64_t flags;
        uintX_t property;
    };

    class Schema {
    protected:
        stringstream& writeEscapeValue(stringstream &ss, string &str);
        unordered_map<typeobj, OracleObject*> objectMap;
        unordered_map<typeobj, OracleObject*> partitionMap;

    public:
        OracleObject *object;
        vector<SchemaElement*> elements;
        unordered_map<uint64_t, SysCCol*> sysCColMap;
        unordered_map<typecol, SysCDef*> sysCDefMap;
        unordered_map<uint64_t, SysCol*> sysColMap;
        unordered_map<typeobj, SysObj*> sysObjMap;
        unordered_map<typeuser, SysUser*> sysUserMap;
        unordered_map<uint64_t, SysECol*> sysEColMap;

        Schema();
        virtual ~Schema();

        bool readSchema(OracleAnalyzer *oracleAnalyzer);
        void writeSchema(OracleAnalyzer *oracleAnalyzer);
        OracleObject *checkDict(typeobj objn, typeobj objd);
        void addToDict(OracleObject *object);
        SchemaElement* addElement(void);
    };
}

#endif
