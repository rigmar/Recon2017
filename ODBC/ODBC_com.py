import sys
import dpkt

from struct import *
import struct
import socket

fDebug = False
if fDebug:
    import pydevd


class ODBC(object):

    def __init__(self,ip,port):
        self.ip = ip
        self.port = port
        self.cli_id = 0xDEADDEAD
        self.hST = 0xC0DEC0DE
        self.databases = []
        self.tables = {}
        self.cur_table = None
        self.handle_arr_idx = 0
        self.dbaTableID = 0
        self.DbCursorIdx = 0
        self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.soc.connect((ip,port))
        buf = self.soc.recv(100)
        #print buf.encode("HEX")


    def GetRunningBases(self):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.cmd = 12
        pkt.obj = dbaLogin()
        #pkt.obj.user = "\0"*32
        #pkt.obj.password = "\0"*32
        #pkt.obj.dbName = "\0"*32
        pkt.obj.set_and_build("","","")
        buf = pkt.build()
        #print dbaPacket(buf)
        buf = pack(">H",len(buf)) + buf
        self.soc.send(buf)
        ans_pkt = self.recivePkt()
        for tab_proc in ans_pkt.obj.aTables:
            self.databases.append(tab_proc.tabName.strip("\0"))
        return self.databases

    def OpenBase(self,user,password,dbName):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.cmd = 10
        pkt.obj = dbaLogin()
        pkt.obj.set_and_build(user,password,dbName)
        buf = pkt.build()
        buf = pack(">H",len(buf)) + buf
        self.soc.send(buf)
        ans_pkt = self.recivePkt()
        #print ans_pkt
        self.handle_arr_idx = ans_pkt.handle_arr_idx
        if ans_pkt.obj is None:
            return (ans_pkt.dbResult, None)

        for tab_proc in ans_pkt.obj.aTables:
            self.tables[tab_proc.tabName.strip("\0")] = tab_proc.TabID
        return (ans_pkt.dbResult, self.tables)

    def OpenTable(self, tabName):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.handle_arr_idx = self.handle_arr_idx
        pkt.cmd = 13
        pkt.obj = dbaTable()
        tabID = self.tables[tabName]
        pkt.obj.set_and_build(tabID,self.hST)
        self.sendPkt(pkt)
        ans_pkt = self.recivePkt()
        self.dbaTableID = ans_pkt.obj.dbaTableID
        self.DbCursorIdx = ans_pkt.obj.DbCursorIdx
        self.cur_table = dbTable(tabName,0,tabID,ans_pkt.obj.ColumnsNum,0,0)
        self.cur_table.dwRecSize = ans_pkt.obj.dwRecSize
        self.cur_table.aCols = [None]*ans_pkt.obj.ColumnsNum
        for col in ans_pkt.obj.aColumns:
            self.cur_table.addColumn(dbColumn(col.ColumName.strip("\0"),col.AccessType,tabID,col.ColID,col.type,col.offset,col.Width))
        for index in ans_pkt.obj.aIndexes:
            self.cur_table.Indexes[index.InxID] = index.aColID
        return  ans_pkt.dbResult

    def SetIndex(self,Idx):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.handle_arr_idx = self.handle_arr_idx
        pkt.cmd = 16
        pkt.obj = dbaSetIndex()
        pkt.obj.set_and_build(self.dbaTableID, self.DbCursorIdx,Idx)
        self.sendPkt(pkt)
        ans_pkt = self.recivePkt()
        return ans_pkt.dbResult

    def SeekFirst(self):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.handle_arr_idx = self.handle_arr_idx
        pkt.cmd = 17
        pkt.obj = dbaManipulate(self.cur_table,cmd=17)
        pkt.obj.set_and_build(self.hST,self.DbCursorIdx,5,0)
        self.sendPkt(pkt)
        ans_pkt = self.recivePkt()
        return ans_pkt

    def MoveNext(self,RecNum):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.handle_arr_idx = self.handle_arr_idx
        pkt.cmd = 18
        pkt.obj = dbaManipulate(self.cur_table,cmd=18)
        pkt.obj.set_and_build(self.hST,self.DbCursorIdx,0,RecNum)
        self.sendPkt(pkt)
        ans_pkt = self.recivePkt()
        return ans_pkt

    def MoveFirst(self,RecNum):
        pkt = dbaPacket()
        pkt.cli_id = self.cli_id
        pkt.handle_arr_idx = self.handle_arr_idx
        pkt.cmd = 18
        pkt.obj = dbaManipulate(self.cur_table,cmd=18)
        pkt.obj.set_and_build(self.hST,self.DbCursorIdx,1,RecNum)
        self.sendPkt(pkt)
        ans_pkt = self.recivePkt()
        return ans_pkt


    def Connect(self,ip,port):
        self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.soc.connect((ip,port))
        buf = self.soc.recv(100)
        if buf == '\0\0':
            return 0
        self.soc.close()
        return 1


    def Close(self):
        self.soc.close()
        self.soc = None
        self.cur_table = None
        self.DbCursorIdx = 0
        self.dbaTableID = 0
        self.handle_arr_idx = 0

    def recivePkt(self):
        size, = unpack(">H",self.soc.recv(2))
        buf = ""
        while len(buf) != size:
            buf += self.soc.recv(0x1000)
        return dbaPacket(buf,cur_table=self.cur_table)

    def sendPkt(self,pkt):
        buf = pkt.build()
        buf = pack(">H",len(buf)) + buf
        self.soc.send(buf)




class dbDatabase(object):
    def __init__(self,name,aTablesProc):
        self.dbName = name
        self.aTablesProc = aTablesProc
        self.aTables = {}
        if len(self.aTablesProc) >0:
            for elem in self.aTablesProc:
                self.add_table(elem)

    def add_table(self,TableProc):
        self.aTables[TableProc.TabID] = dbTable(TableProc.tabName.strip("\0"),0,TableProc.TabID,0,0,0)

    def update_table(self,Table):
        if Table.tabID in self.aTables:
            dbTab = self.aTables[Table.tabID]
            dbTab.aCols = [None]*Table.ColumnsNum
            dbTab.NumCol = Table.ColumnsNum
            dbTab.dwRecSize = Table.dwRecSize
            for col in Table.aColumns:
                #print col.ColID
                dbTab.addColumn(dbColumn(col.ColumName.strip("\0"),col.AccessType,Table.tabID,col.ColID,col.type,col.offset,col.Width))



class dbaRecord(object):
    Len = 0x20
    def __init__(self,cur_table,data = None):
        self.fmt = ">IHHIIIIII"
        self.data = data
        self.dbaMemberType = 8

        self.offset = 0
        self.dwRecSize = 0
        self.dw1 = 0
        self.dw2 = 0
        self.dw3 = 0
        self.flags = 0
        self.Null = 0
        self.RecID = 0
        self.rec = ""

        self.ptr = 0

        self.cur_table = cur_table

        if data is not None:
            self.dbaMemberType, self.offset, self.dwRecSize, self.dw1, self.dw2, self.dw3, self.flags, self.Null, self.RecID = unpack(self.fmt,self.data[self.ptr:self.ptr + dbaRecord.Len])
            self.ptr += dbaRecord.Len
            self.rec = self.data[self.ptr:self.ptr + self.dwRecSize]
            self.ptr += self.dwRecSize

    def __repr__(self):
        ret = ""
        ret += "\nself.dbaMemberType = %d\nself.offset = %X\nself.dwRecSize = %d\nself.dw1 = %04X\nself.dw2 = %04X\nself.dw3 = %04X\nself.flags = %X\nself.Null = %X\nself.RecID = %X"%(self.dbaMemberType, self.offset, self.dwRecSize, self.dw1, self.dw2, self.dw3, self.flags, self.Null, self.RecID)
        ret += "\nRecord:\n"
        for elem in self.cur_table.aCols:
            ret += "\n\t%s = %s"%(elem.ColName,elem.parse(self.rec))
        return ret

    def GetLen(self):
        return dbaRecord.Len + self.dwRecSize

    def build(self):
        return pack(self.fmt,self.dbaMemberType, self.offset, self.dwRecSize, self.dw1, self.dw2, self.dw3, self.flags, self.Null, self.RecID) + self.rec

    def set_and_build(self,dwRecSize,rec):
        self.dwRecSize = dwRecSize
        self.rec = rec
        return self.build()

class dbColumn(object):
    aTypes = {
        0:  (4,  "ROWID", "L"),
        1:  (4,  "UI32",  "L"),
        2:  (2,  "UI16",  "H"),
        3:  (1,  "UI8",   "B"),
        4:  (4,  "I32",   "l"),
        5:  (2,  "I16",   "h"),
        6:  (1,  "I8",    "b"),
        7:  (4,  "FLT",   "f"),
        8:  (8,  "DBL",   "d"),
        9:  (0,  "STR",   ""),
        10: (0,  "BLK",   ""),
        11: (0,  "USR",   ""),
        12: (0,  "BLOB",  ""),
        13: (12, "TIME",  "12s"),
        14: (0,  "OID",   ""),
        16: (8,  "UI64",  "Q"),
        17: (8,  "I64",   "q"),
        18: (12, "SPAN",  "12s"),
        19: (12, "DATE",  "12s"),
        20: (4,  "SPA",   "4s"),
        21: (1,  "CHR",   "c"),
        22: (32, "SIG",   "32s"),
        23: (4,  "SST",   "4s"),
        40: (12, "DBV",   "12s"),
    }

    def __init__(self, ColName, Access, TabId, ColId, Type, Offset, Width):
        self.ColName = ColName
        self.Access = Access
        self.TabId = TabId
        self.ColId = ColId
        self.Type = Type
        self.Offset = Offset
        self.Width = Width
        self.cb, self.typeName, self.fmt = self.aTypes[self.Type]

    def parse(self, data):
        r = data[self.Offset:self.Offset+self.Width]
        #print r.encode("HEX")
        if self.cb: assert len(r) == self.cb # Check length for fixed-len fields
        if self.fmt: r, = struct.unpack(">" + self.fmt, r)
        if "STR" == self.typeName:
            r = r.rstrip("\0")
            if len(r) == 0:
                r = "Empty string"
        if isinstance(r, str) and self.typeName not in ("STR", "CHR", ): r = "[%s]" % r.encode("hex")
        self.v = r
        return r

class dbTable(object):
    def __init__(self, TabName, Access, TabId, NumCol, AutoBase, Flags):
        self.TabName = TabName
        self.Access = Access
        self.TabId = TabId
        self.NumCol = NumCol
        self.AutoBase = AutoBase
        self.Flags = Flags
        self.aCols = [None]*NumCol
        self.Indexes = {}
        self.aColNames = {}
        self.dwRecSize = 0

    def addColumn(self, col):
        iCol = col.ColId - 1
        assert self.aCols[iCol] is None
        self.aCols[iCol] = col
        assert col.ColName not in self.aColNames
        self.aColNames[col.ColName] = iCol

    def dumpDesc(self):
        print "TabId=%04X Access=%04X AutoBase=%d Flags=%d NumCol=%2d [%s]" % (self.TabId, self.Access, self. AutoBase, self.Flags, self.NumCol, self.TabName)
        for col in self.aCols:
            print "  %2d: Access=%04X Type=%X (%5s) Offset=%3X Width=%2X [%s]" % (col.ColId, col.Access, col.Type, col.typeName, col.Offset, col.Width, col.ColName)

    def parseRec(self, data):
        self.vals = {}
        for col in self.aCols:
            self.vals[col.ColName] = col.parse(data)

    def dumpTable(self, db):
        self.dumpDesc()
        print "\t".join("%s %s" % (col.ColName, col.typeName) for col in self.aCols)
        for recInfo, data in db.recWalker(self.TabId):
            self.parseRec(data)
            print "%5d: %s %08X %08X %08X %s" % (recInfo[0], recInfo[1].encode("hex"), recInfo[2], recInfo[3], recInfo[4], "\t".join(str(self.vals[col.ColName]) for col in self.aCols))



class dbaManipulate(object):
    Len = 0x10

    seek_cmds = {
        5:"seek:First",
        6:"seek:Last",
        0:"seek:EQ",
        1:"seek:GT",
        2:"seek:LT",
        3:"seek:LE",
        4:"seek:GE"
    }

    move_cmds = {
        0:"move:Next",
        1:"move:First",
        2:"move:Current"
    }

    def __init__(self,cur_table ,data = None,cmd = 0):
        self.fmt = ">IIIBBH"
        self.data = data
        self.dbaMemberType = 7

        self.hST = 0
        self.dbCursorIdx = 0
        self.oper = 0
        self.b1 = 0
        self.RecNum = 0
        self.cmd = cmd
        self.aRecords = []
        self.dwRecSize = 0
        self.rec_data = ""
        self.cur_table = cur_table
        self.dwRecSize = cur_table.dwRecSize
        self.ptr = 0

        if data is not None:
            self.dbaMemberType, self.hST, self.dbCursorIdx, self.oper, self.b1, self.RecNum = unpack(self.fmt,self.data[self.ptr:self.ptr + dbaManipulate.Len])
            self.ptr += dbaManipulate.Len
            if self.cmd in (17,18) and len(self.data[self.ptr:]):
                for i in range(self.RecNum):
                    record = dbaRecord(self.cur_table, self.data[self.ptr:])
                    #self.dwRecSize = record.dwRecSize
                    self.ptr += record.ptr
                    self.aRecords.append(record)
            elif self.cmd == 20 and len(self.data[self.ptr:]):
                record = dbaRecord(self.cur_table, self.data[self.ptr:])
                #self.dwRecSize = record.dwRecSize
                self.aRecords.append(record)


    def __repr__(self):
        ret = ""
        sOper = ""
        ptr = 0
        if self.cmd == 17:
            sOper = dbaManipulate.seek_cmds[self.oper]
        elif self.cmd == 18:
            sOper = dbaManipulate.move_cmds[self.oper]
        ret += "\ndbaManipulate:\nself.dbaMemberType = %d\nself.hST = %X\nself.dbCursorIdx = %X\nself.oper = %d (%s)\nself.b1 = %d\nself.RecNum = %d"%(self.dbaMemberType, self.hST, self.dbCursorIdx, self.oper, sOper, self.b1, self.RecNum)
        for i,elem in enumerate(self.aRecords,1):
            if i == 1:
                ret += "\ndbaRecords:"
            ret += "\n\t%d Record:\n"%i + ("%s"%elem).replace("\n","\n\t\t") + "\n"
        return ret

    def GetLen(self):
        return dbaManipulate.Len + len(self.aRecords) * dbaRecord.Len + len(self.aRecords) * self.dwRecSize

    def build(self):
        buf = ""
        buf += pack(self.fmt,self.dbaMemberType, self.hST, self.dbCursorIdx, self.oper, self.b1, self.RecNum)
        for rec in self.aRecords:
            buf += rec.build()
        return buf

    def set_and_build(self,hST,dbCursorIdx,oper,RecNum,records = []):
        self.hST = hST
        self.dbCursorIdx = dbCursorIdx
        self.oper = oper
        self.aRecords = records
        self.RecNum = RecNum
        return self.build()


class dbaSetIndex(object):
    Len = 0x10
    def __init__(self,data = None):
        self.fmt = ">IIII"
        self.data = data
        self.dbaMemberType = 6

        self.dbaTableID = 0
        self.dbCursorIdx = 0
        self.IndexIdx = 0

        self.ptr = 0

        if data is not None:
            self.dbaMemberType, self.dbaTableID, self.dbCursorIdx, self.IndexIdx = unpack(self.fmt,self.data[self.ptr:self.ptr + dbaSetIndex.Len])
            self.ptr += dbaSetIndex.Len

    def __repr__(self):
        ret = ""
        ret += "\ndbaSetIndex:\nself.dbaMemberType = %d\nself.dbaTableID = %X\nself.dbCursorIdx = %X\nself.IndexIdx = %d"%(self.dbaMemberType, self.dbaTableID, self.dbCursorIdx, self.IndexIdx)
        return ret

    def build(self):
        return pack(self.fmt,self.dbaMemberType, self.dbaTableID, self.dbCursorIdx, self.IndexIdx)

    def set_and_build(self,dbaTableID,dbCursorIdx,Idx):
        self.dbaTableID = dbaTableID
        self.dbCursorIdx = dbCursorIdx
        self.IndexIdx = Idx
        return pack(self.fmt,self.dbaMemberType, self.dbaTableID, self.dbCursorIdx, self.IndexIdx)

    def GetLen(self):
        return dbaSetIndex.Len

class dbaIndex(object):
    Len = 0x4c
    def __init__(self,data = None):
        self.fmt = ">I32sI32BI"
        self.data = data
        self.dbaMemberType = 5

        self.IndexName = ""
        self.AccessType = 0
        self.aColID = ()
        self.InxID = 0

        self.ptr = 0

        if data is not None:
            args = unpack(self.fmt,self.data[self.ptr:self.ptr + dbaIndex.Len])
            self.dbaMemberType = args[0]
            self.IndexName = args[1]
            self.AccessType = args[2]
            self.aColID = args[3:3+32]
            self.InxID = args[3+32]
            self.aColID = list(self.aColID)
            self.ptr += dbaIndex.Len

    def __repr__(self):
        ret = ""
        ret += "\ndbaIndex:\nself.dbaMemberType = %d\nself.IndexName = %s\nself.AccessType = %d\nself.aColID = %s\nself.InxID = %d"%(self.dbaMemberType, self.IndexName.strip("\0"), self.AccessType, self.aColID, self.InxID)
        return ret


class dbaColumn(object):
    Len = 0x38
    def __init__(self,data = None):
        self.fmt = ">I32sHHHHHHBBHI"
        self.data = data
        self.dbaMemberType = 4
        self.ColumName = ""
        self.type = 0
        self.AccessType = 0
        self.Width = 0
        self.offset = 0
        self.half1 = 0
        self.half2 = 0
        self.ColID = 0
        self.b1 = 0
        self.half3 = 0
        self.RecID = 0

        self.ptr = 0

        if data is not None:
            self.dbaMemberType, self.ColumName, self.type, self.AccessType, self.Width, self.offset, self.half1, self.half2, self.ColID, self.b1, self.half3, self.RecID = unpack(self.fmt,self.data[self.ptr:self.ptr + dbaColumn.Len])
            self.ptr += dbaColumn.Len

    def __repr__(self):
        ret = ""
        ret += "\ndbaColumn:\nself.dbaMemberType = %d\nself.ColumName = %s\nself.type = %X\nself.AccessType = %d\nself.Width = %X\nself.offset = %X\nself.half1 = %02X\nself.half2 = %02X\nself.ColID = %d\nself.b1 = %d\nself.half3 = %02X\nself.RecID = %d"%(self.dbaMemberType, self.ColumName.strip("\0"), self.type, self.AccessType, self.Width, self.offset, self.half1, self.half2, self.ColID, self.b1, self.half3, self.RecID)
        return ret


class dbaTable(object):
    Len = 0x1c

    def __init__(self,data = None):
        self.fmt = ">IIIIIHBBBBBB"
        self.data = data
        self.dbaMemberType = 3
        self.hST = 0
        self.dbaTableID = 0
        self.tabID = 0
        self.DbCursorIdx = 0
        self.dwRecSize = 0
        self.b1 = 0
        self.ColumnsNum = 0
        self.IndexNum = 0
        self.b2 = 0
        self.b3 = 0
        self.b4 = 0

        self.ptr = 0
        self.aColumns = []
        self.aIndexes = []

        if data is not None:
            self.dbaMemberType, self.hST, self.dbaTableID, self.tabID, self.DbCursorIdx, self.dwRecSize, self.b1, self.ColumnsNum, self.IndexNum, self.b2, self.b3, self.b4 = unpack(self.fmt,self.data[self.ptr:self.ptr + dbaTable.Len])
            self.ptr += dbaTable.Len
            if self.ColumnsNum > 0:
                for i in range(self.ColumnsNum):
                    self.aColumns.append(dbaColumn(self.data[self.ptr:self.ptr + dbaColumn.Len]))
                    self.ptr += dbaColumn.Len
            if self.IndexNum > 0:
                for i in range(self.IndexNum):
                    self.aIndexes.append(dbaIndex(self.data[self.ptr:self.ptr + dbaIndex.Len]))
                    self.ptr += dbaIndex.Len

    def __repr__(self):
        ret = ""
        ret += "\ndbaTable:\n\nself.dbaMemberType = %d\nself.hST = %04X\nself.dbaTableID = %d\nself.tabID = %d\nself.DbCursorIdx = %04X\nself.dwRecSize = %d (%d)\nself.b1 = %d\nself.ColumnsNum = %d\nself.IndexNum = %d\nself.b2 = %d\nself.b3 = %d\nself.b4 = %d\n"%(self.dbaMemberType, self.hST, self.dbaTableID, self.tabID, self.DbCursorIdx, self.dwRecSize, self.dwRecSize*4, self.b1, self.ColumnsNum, self.IndexNum, self.b2, self.b3, self.b4)
        ret += "\n"
        for i,elem in enumerate(self.aColumns,1):
            if i == 1:
                ret += "dbaColumns:\n"
            ret += "\n\t%d "%i + ("%s"%elem).replace("\n","\n\t\t") + "\n"
        for i,elem in enumerate(self.aIndexes,1):
            if i == 1:
                ret += "dbaIndexes:\n"
            ret += "\n\t%d "%i + ("%s"%elem).replace("\n","\n\t\t") + "\n"

        return ret

    def GetLen(self):
        return dbaTable.Len + self.ColumnsNum * dbaColumn.Len + self.IndexNum * dbaIndex.Len

    def build(self):
        return pack(self.fmt,self.dbaMemberType, self.hST, self.dbaTableID, self.tabID, self.DbCursorIdx, self.dwRecSize, self.b1, self.ColumnsNum, self.IndexNum, self.b2, self.b3, self.b4)

    def set_and_build(self,tabID,hST):
        self.tabID = tabID
        self.hST = hST
        return pack(self.fmt,self.dbaMemberType, self.hST, self.dbaTableID, self.tabID, self.DbCursorIdx, self.dwRecSize, self.b1, self.ColumnsNum, self.IndexNum, self.b2, self.b3, self.b4)

class dbaLogin(object):
    Len = 0x6c
    def __init__(self,data = None):
        self.fmt = ">I32s32s32sHHHH"
        self.data = data
        self.dbaMemberType = 1
        self.user = ""
        self.password = ""
        self.dbName = ""
        self.tables_num = 0
        self.p = 0
        self.atomTransNum = 0
        self.half1 = 0
        self.ptr = 0
        self.aTables = []
        self.aAtomTrans = []
        if data is not None:
            self.dbaMemberType, self.user, self.password, self.dbName, self.tables_num, self.p, self.atomTransNum, self.half1 = unpack(self.fmt,self.data[self.ptr:self.ptr + 0x6c])
            self.ptr += 0x6c
            if self.tables_num > 0:
                for i in range(self.tables_num):
                    self.aTables.append(dbaTableProc(self.data[self.ptr:self.ptr + dbaTableProc.Len]))
                    self.ptr += dbaTableProc.Len
            if self.atomTransNum > 0:
                for i in range(self.atomTransNum):
                    self.aAtomTrans.append(dbaAtomTrans(self.data[self.ptr:self.ptr + dbaAtomTrans.Len]))
                    self.ptr += dbaAtomTrans.Len

    def GetLen(self):
        return dbaLogin.Len + self.tables_num*dbaTableProc.Len + self.atomTransNum*dbaAtomTrans.Len

    def __repr__(self):
        ret = "\ndbaLogin:\ndbaMemberType = %d\nself.user = %s\nself.password = %s\nself.dbName = %s\nself.tables_num = %d\nself.p = %X\nself.atomTransNum = %d\nself.half1 = %X"%(self.dbaMemberType, self.user.strip("\0"), self.password.strip("\0"), self.dbName.strip("\0"), self.tables_num, self.p, self.atomTransNum, self.half1)
        ret +="\n"
        for i,elem in enumerate(self.aTables,1):
            if i == 1:
                ret += "dbaTables:\n"
            ret += "\n\t%d "%i + ("%s"%elem).replace("\n","\n\t\t") + "\n"
        for i,elem in enumerate(self.aAtomTrans,1):
            if i == 1:
                ret += "dbaAtomTrans:\n"
            ret += "\n\t%d "%i + ("%s"%elem).replace("\n","\n\t\t") + "\n"
        return ret

    def build(self):
        return pack(self.fmt,self.dbaMemberType, self.user, self.password, self.dbName, self.tables_num, self.p, self.atomTransNum, self.half1)

    def set_and_build(self,user,password,dbName):
        self.user = user + "\0"*(32 - len(user))
        self.password = password + "\0"*(32 - len(password))
        self.dbName = dbName + "\0"*(32 - len(dbName))
        return pack(self.fmt,self.dbaMemberType, self.user, self.password, self.dbName, self.tables_num, self.p, self.atomTransNum, self.half1)


class dbaTableProc(object):
    Len = 0x34
    def __init__(self,data = None):
        self.data = data
        self.dbaMemberType = 2
        self.tabName = ""
        self.fPrcType = 0
        self.R = 0
        self.P = 0
        self.TabID = 0
        if data is not None:
            self.dbaMemberType, self.tabName, self.fPrcType, self.R, self.P, self.TabID = unpack(">I32sIIII",data[:0x34])

    def __repr__(self):
        ret = "\ndbaTable:\ndbaMemberType = %d\nself.tabName = %s\nself.fPrcType = %d\nself.R = %X\nself.P = %X\nself.TabID = %d\n"%(self.dbaMemberType, self.tabName.strip("\0"), self.fPrcType, self.R, self.P, self.TabID)
        return ret

class dbaAtomTrans(object):
    Len = 0x28
    def __init__(self,data = None):
        self.data = data
        self.dbaMemberType = 21
        self.Text = ""
        self.Atom = 0
        self.b1 = 0
        if data is not None:
            self.dbaMemberType,self.Atom,self.Text,self.b1 = unpack(">LL31sB",data[:0x28])

    def __repr__(self):
        ret = "\ndbaAtomTrans:\n\ndbaMemberType = %d\nself.Atom = %X\nself.Text = %s\nself.b1 = %X\n"%(self.dbaMemberType,self.Atom,self.Text.strip("\0"),self.b1)
        return ret


class dbaPacket(object):
    commands ={
        10:"Start Database",
        11:"Close Database",
        12:"List running database",
        13:"Table Open",
        14:"Table ReOpen",
        15:"Table Close",
        16:"Set Index",
        17:"Seek",
        18:"Move",
        19:"Goto",
        20:"StartTransactionC:insert",
        21:"StartTransactionC:remove",
        22:"Blob Read",
        23:"Blob Write",
        24:"Add table",
        25:"Get table??",
        26:"Begin transaction",
        27:"Commit transaction",
        28:"RollbackTransaction1",
        29:"ActivateTrans",
        30:"RollbackTransaction2"
    }
    Len = 20

    def __init__(self,data = None,cur_table = None):
        self.fmt = ">3sBHHIII"
        self.dwMagic ="03a101".decode("HEX")
        self.b1 = 1
        self.len = 0
        self.cmd = 0
        self.cli_id = 0
        self.handle_arr_idx = 0
        self.dbResult = 0
        self.data = data
        self.cbParsed = 0
        self.obj = None
        self.cur_table = cur_table
        if self.data is not None:
            self.dwMagic, self.b1, self.len, self.cmd, self.cli_id, self.handle_arr_idx, self.dbResult = unpack(self.fmt,self.data[:20])
            self.data = self.data[20:]
            self.cbParsed += 20
            if len(self.data) > 0:
                if self.cmd in (10,11,12,26,27,28,29,30):
                    self.obj = dbaLogin(self.data)
                    self.cbParsed += dbaLogin.Len
                    self.cbParsed += len(self.obj.aTables) * dbaTableProc.Len
                    self.cbParsed += len(self.obj.aAtomTrans) * dbaAtomTrans.Len
                elif self.cmd in (13,14,15):
                    self.obj = dbaTable(self.data)
                    self.cbParsed += dbaTable.Len
                    self.cbParsed += len(self.obj.aColumns) * dbaColumn.Len
                    self.cbParsed += len(self.obj.aIndexes) * dbaIndex.Len
                elif self.cmd == 16:
                    if len(self.data) >= 16:
                        self.obj = dbaSetIndex(self.data)
                        self.cbParsed += dbaSetIndex.Len
                elif self.cmd in (17,18,19,20,21,22,23):
                    self.obj = dbaManipulate(self.cur_table,self.data,self.cmd)
                    self.cbParsed += dbaManipulate.Len
                    if self.cmd == 20:
                        self.cbParsed += dbaRecord.Len
                        self.cbParsed += self.obj.dwRecSize
                    else:
                        self.cbParsed += self.obj.RecNum * dbaRecord.Len
                        self.cbParsed += self.obj.RecNum * self.obj.dwRecSize


    def __repr__(self):
        if self.cmd in dbaPacket.commands:
            sCmd = dbaPacket.commands[self.cmd]
        else:
            sCmd = "Unk"
        ret = "dbaPacket:\n\nself.dwMagic = %s\nself.b1 = %X\nself.len = %d\nself.cmd = %d (%s)\nself.cli_id = %X\nself.handle_arr_idx = %X\nself.dbResult= %X\nself.cbParsed = %d\n"%(self.dwMagic.encode("HEX"), self.b1, self.len, self.cmd, sCmd, self.cli_id, self.handle_arr_idx, self.dbResult,self.cbParsed)
        ret += ("%s"%self.obj).replace("\n","\n\t")
        if self.cbParsed != self.len:
            ret += "\n%d\nData:\n["%len(self.data[self.cbParsed - 20:])
            for ch in self.data[self.cbParsed - 20:]:
                ret += " %s"%ch.encode("HEX")
            ret += "]"
        return ret


    def build(self):
        self.len = dbaPacket.Len
        if self.obj is not None:
            self.len += self.obj.GetLen()
        buf = ""
        buf += pack(self.fmt,self.dwMagic, self.b1, self.len, self.cmd, self.cli_id, self.handle_arr_idx, self.dbResult)
        #print
        if self.obj is not None:
            self.len += self.obj.GetLen()
            buf += self.obj.build()
        return buf




