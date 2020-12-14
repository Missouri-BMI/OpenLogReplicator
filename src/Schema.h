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

    struct SysUser {
        string rowid;
        typeUSER user;      //pk
        string name;
        uint64_t spare1;
    };

    struct SysObj {
        string rowid;
        typeUSER owner;
        typeOBJ obj;       //pk
        typeDATAOBJ dataObj;
        uint64_t type;
        string name;
        uint32_t flags;
    };

    struct SysCol {
        string rowid;
        typeOBJ obj;       //pk
        typeCOL col;
        typeCOL segCol;
        typeCOL intCol;     //pk
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

    struct SysCCol {
        string rowid;
        typeCON con;        //pk
        typeCOL intCol;     //pk
        typeOBJ obj;
        uint64_t spare1;
    };

    struct SysCDef {
        string rowid;
        typeCON con;        //pk
        typeOBJ obj;
        uint64_t type;
    };

    struct SysDeferredStg {
        string rowid;
        typeOBJ obj;       //pk
        uint64_t flagsStg;
    };

    struct SysECol {
        string rowid;
        typeOBJ obj;
        uint32_t colNum;
        uint32_t guardId;
    };

    struct SysSeg {
        string rowid;
        uint32_t file;
        uint32_t block;
        uint32_t ts;
        uint64_t spare1;
    };

    struct SysTab {
        string rowid;
        typeOBJ obj;       //pk
        typeDATAOBJ dataObj;
        uint32_t ts;
        uint32_t file;
        uint32_t block;
        uint64_t cluCols;
        uint64_t flags;
        uintX_t property;
    };

    struct SysTabPart {
        string rowid;
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeOBJ bo;
    };

    struct SysTabComPart {
        string rowid;
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeOBJ bo;
    };

    struct SysTabSubPart {
        string rowid;
        typeOBJ obj;
        typeDATAOBJ dataObj;
        typeOBJ pobj;
    };

    class Schema {
    protected:
        stringstream& writeEscapeValue(stringstream &ss, string &str);
        unordered_map<typeOBJ, OracleObject*> objectMap;
        unordered_map<typeOBJ, OracleObject*> partitionMap;

    public:
        OracleObject *object;
        vector<SchemaElement*> elements;

        Schema();
        virtual ~Schema();

        bool readSchema(OracleAnalyzer *oracleAnalyzer);
        void writeSchema(OracleAnalyzer *oracleAnalyzer);
        OracleObject *checkDict(typeOBJ obj, typeDATAOBJ dataObj);
        void addToDict(OracleObject *object);
        SchemaElement* addElement(void);
        bool dictSysUserAdd(const char *rowid, typeUSER user, const char *name, uint64_t spare1);
        bool dictSysObjAdd(const char *rowid, typeUSER owner, typeOBJ obj, typeDATAOBJ dataObj, uint64_t type, const char *name, uint32_t flags);
        bool dictSysColAdd(const char *rowid, typeOBJ obj, typeCOL col, typeCOL segCol, typeCOL intCol, const char *name, uint64_t type, uint64_t length,
                int64_t precision, int64_t scale, uint64_t charsetForm, uint64_t charsetId, int64_t null, uint64_t property1, uint64_t property2);
        bool dictSysCColAdd(const char *rowid, typeCON con, typeCOL intCol, typeOBJ obj, uint64_t spare1);
        bool dictSysCDefAdd(const char *rowid, typeCON con, typeOBJ obj, uint64_t type);
        bool dictSysDeferredStg(const char *rowid, typeOBJ obj, uint64_t flagsStg);
        bool dictSysECol(const char *rowid, typeOBJ obj, uint32_t colNum, uint32_t guardId);
        bool dictSysSeg(const char *rowid, uint32_t file, uint32_t block, uint32_t ts, uint64_t spare1);
        bool dictSysTab(const char *rowid, typeOBJ obj, typeDATAOBJ dataObj, uint32_t ts, uint32_t file, uint32_t block, uint64_t cluCols,
                uint64_t flags, uint64_t property1, uint64_t property2);
        bool dictSysTabPart(const char *rowid, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabComPart(const char *rowid, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ bo);
        bool dictSysTabSubPart(const char *rowid, typeOBJ obj, typeDATAOBJ dataObj, typeOBJ pobj);
    };
}

#endif
