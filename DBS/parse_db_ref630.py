import os, sys, struct, zlib, datetime
from Crypto.Cipher import Blowfish
fDebug = False
if fDebug:
    import pydevd


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
        if self.cb: assert len(r) == self.cb # Check length for fixed-len fields
        if self.fmt: r, = struct.unpack(">" + self.fmt, r)
        if "STR" == self.typeName: r = r.replace('\0', ' ')# r.rstrip("\0")
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
        self.aColNames = {}

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

class dbStreamHeader(object):
    def __init__(self,obj):
        self.raw = obj.read(0x100)
        self.sig1,self.size,self.crc = struct.unpack(">III",self.raw[:12])
        self.sig2, = struct.unpack(">I",self.raw[-4:])
        
    def __repr__(self):
        return "dbStreamHeader:\n\tsig1 = %08X, size = 0x%08X (%d), crc = %08X, sig2 = %X\n"%(self.sig1, self.size, self.size, self.crc, self.sig2)
        
class BasicBlockHeader(object):
    def __init__(self,obj):
        self.validFlag, self.ChkSum = struct.unpack(">II",obj.read(8))
        
    def __repr__(self):
        return "BasicBlockHeader:\n\tvalidFlag = %X, ChkSum = %08X\n"%(self.validFlag, self.ChkSum)

class PartionHeader(object):
    def __init__(self,obj):
        self.sig, self.modeBits, self.ndwBlocksize, self.nBlk = struct.unpack(">IIII",obj.read(16))
        self.cbBlocksize = self.ndwBlocksize*4
        
    def __repr__(self):
        return "PartionHeader:\n\tsig = %08X, modeBits = %X, blocksize (in DWORDS) = %X (%d), blocksize (in BYTES) = %X (%d), nBlocks = %X (%d)\n"%(self.sig, self.modeBits, self.ndwBlocksize, self.ndwBlocksize, self.ndwBlocksize*4, self.ndwBlocksize*4, self.nBlk, self.nBlk)

class BlockHeader(object):
    def __init__(self,raw):
        self.raw = raw
        self.sig, self.HdrType, self.bState, self.blkIndex, self.ndwRecSize, self.ownId, self.wUnk = struct.unpack(">3sBiHHHH",self.raw[:16])
        
    def __repr__(self):
        return "BlockHeader\n\tself.sig = %s, self.HdrType = %d, self.bState = %d, self.blkIndex = %X (%d), self.ndwRecSize = %X, self.ownId = %d, self.wUnk = %X\n"%(self.sig.encode("HEX"), self.HdrType, self.bState, self.blkIndex, self.blkIndex, self.ndwRecSize, self.ownId, self.wUnk)

class dbBasicBlock(object):
    def __init__(self,obj):
        self.BasicBlockHdr = BasicBlockHeader(obj)
        self.raw = obj.read(obj.PartionHdr.cbBlocksize)
        self.BlockHdr = BlockHeader(self.raw)
        self.abData = self.raw[16:]
    
    def __repr__(self):
        s = "Block number %d\n\t"%self.BlockHdr.blkIndex
        s += self.BasicBlockHdr.__repr__() + "\n\t"
        s += self.BlockHdr.__repr__() + "\n"
        return s
        
class dbSeqBlock(object):
    
    key = '27397a48561c45445259546f6b1a7f7062777b7f7d732e323534'.decode("HEX")

    def __init__(self,obj):
        self.CompressBlockSize = struct.unpack(">H",obj.read(2))[0]
        # print "0x%X"%self.CompressBlockSize
        self.raw = obj.read(self.CompressBlockSize)
        # print len(self.raw)
        if obj.PartionHdr.modeBits&0x1000:
            # print "crypted"
            self.raw = self.DoDecrypt(self.raw)
        print "dbSeqBlock: Compress Block Size = 0x%04X"%self.CompressBlockSize
        # print len(self.raw)
        # print "0x%X"%struct.unpack(">I",self.raw[-4:])[0]
        
            
        self.raw = zlib.decompress(self.raw)
        print self.raw.encode("hex")
        print len(self.raw)
        self.BlockHdr = BlockHeader(self.raw)
        self.abData = self.raw[16:]
        
    def DoDecrypt(self,data):
        plainLen = struct.unpack(">I",data[-4:])[0]
        # print "PlainLen = 0x%X"%plainLen
        data = data[:-4]
        # print len(data)
        # print data.encode("hex")
        assert len(data)%8 == 0
        cipher = Blowfish.new(dbSeqBlock.key, Blowfish.MODE_ECB)
        ret = ""
        i = 0
        while True:
            # print i
            # print data[i:i+8].encode("hex")
            plain = cipher.decrypt(data[i:i+8])
            # print plain[:4].encode("hex")
            plain = struct.pack(">I",struct.unpack(">I",plain[:4])[0] ^ ((~i)&0xffffffff)) + plain[4:]
            ret += plain
            i += 8
            # print i
            if i >= plainLen:
                break
        return ret
    
    def __repr__(self):
        s = "Block number %d\n\t"%self.BlockHdr.blkIndex
        s += "Compress Block Size = 0x%04X"%self.CompressBlockSize + "\n\t"
        s += self.BlockHdr.__repr__() + "\n"
        return s
    
class dbFile(object):
    TID_SYS_TABLE = 1
    TID_SYS_COLUMN = 2
    TID_SYS_INDEX = 3
    TID_SYS_USER = 4

    def read(self, cb):
        data = self.ab[self.o:self.o + cb]
        assert len(data) == cb
        self.o += cb
        return data
               
    def recWalker(self, iTab):
        iRec = -1
        for iBlk, blk in enumerate(self.aBlocks):
          if blk.BlockHdr.ownId != iTab: continue
          if not len(blk.abData): continue
          cbRec = blk.BlockHdr.ndwRecSize*4
          for o in xrange(0, len(blk.abData)-cbRec+1, cbRec):
            iRec += 1
            rec = blk.abData[o:o+cbRec]
            u12, dw1, dw2, dw3 = struct.unpack_from(">12sLLL", rec)
            data = rec[24:]
            if data == "\xAB" * len(data): continue # Empty record
            if data == "\0" * len(data): continue
            yield((iRec, u12, dw1, dw2, dw3), data)
    
    def dumpRec(self):
        iRec = -1
        for iBlk, blk in enumerate(self.aBlocks):
          if not len(blk.abData): continue
          cbRec = blk.BlockHdr.ndwRecSize*4
          for o in xrange(0, len(blk.abData)-cbRec+1, cbRec):
            iRec += 1
            rec = blk.abData[o:o+cbRec]
            u12, dw1, dw2, dw3 = struct.unpack_from(">12sLLL", rec)
            data = rec[24:]
            if data == "\xAB" * len(data): continue # Empty record
            yield((iRec, u12, dw1, dw2, dw3), data)
  
    def dumpTabDesc(self):
        for tabId in sorted(self.dTabs):
          self.dTabs[tabId].dumpTable(self)
          print '-'*77
        print "="*77
    
    def dump(self):
        if self.seq:
            print "%08X %s\n %s %s %s" % (self.csum, self.fn, self.StreamHdr, "FirstBlock size = 0x%04X\n"%self.CompressBlockSize, self. PartionHdr)
        else:
            print "%08X %s\n %s %s %s" % (self.csum, self.fn, self.StreamHdr, self.BasicBlockHdr, self. PartionHdr)
        #    for i in xrange(self.hdr.nBlk):
        #      print "  %s" % self.dBlk[i]
    
    def __init__(self, fn):
        self.fn = fn
        base, ext = os.path.splitext(fn)
        self.seq = False
        with open(fn, "rb") as f:
          if ext.lower() == ".db":
            cb, self.csum = struct.unpack(">LL", f.read(8))
            self.seq = True
          else:
            cb = os.path.getsize(fn)
            self.csum = 0xDEADC0DE;
          self.ab = f.read()
        assert len(self.ab) == cb
        self.o = 0
        
        self.StreamHdr = dbStreamHeader(self)
        if self.seq:
            self.CompressBlockSize = struct.unpack(">H",self.read(2))[0]
            self.PartionHdr = PartionHeader(self)
            print self.StreamHdr, "FirstBlock size = 0x%04X\n"%self.CompressBlockSize, self. PartionHdr
            self.o += self.PartionHdr.cbBlocksize - 16
            
        else:
            self.BasicBlockHdr = BasicBlockHeader(self)
            self.PartionHdr = PartionHeader(self)
            print self.StreamHdr, self.BasicBlockHdr, self. PartionHdr
            self.o += self.PartionHdr.cbBlocksize - 16
        self.aBlocks = [None]*self.PartionHdr.nBlk
        for i in xrange(self.PartionHdr.nBlk):
            if self.seq:
                blk = dbSeqBlock(self)
            else:
                blk = dbBasicBlock(self)
            print blk
            #print "%08X" % self.o, blk
            assert self.aBlocks[blk.BlockHdr.blkIndex] is None
            self.aBlocks[blk.BlockHdr.blkIndex] = blk 
        # Load list of tables
        self.dTabs = {}
        self.dTabNames = {}
        for recInfo, data in self.recWalker(self.TID_SYS_TABLE):
          TabName, Access, TabId, NumCol, AutoBase, Flags = struct.unpack(">32sLHHLL", data)
          TabName = TabName.rstrip('\0')
          print "TabName = %s, Access = %X, TabId = %d, NumCol = %d, AutoBase = %X, Flags = %X"%(TabName, Access, TabId, NumCol, AutoBase, Flags)
          tab = dbTable(TabName, Access, TabId, NumCol, AutoBase, Flags)
          assert TabId not in self.dTabs
          self.dTabs[TabId] = tab
          assert TabName not in self.dTabNames
          self.dTabNames[TabName] = TabId
          
        for recInfo, data in self.recWalker(self.TID_SYS_COLUMN):
          ColName, Access, TabId, ColId, Type, Offset, Width, unused = struct.unpack(">32sLHHHHHH", data)
          ColName = ColName.rstrip('\0')
          print "ColName = %s, Access = 0x%X, TabId = %d, ColId = %d, Type = %d, Offset = %d, Width = %d"%(ColName, Access, TabId, ColId, Type, Offset, Width)
          tab = self.dTabs[TabId]
          tab.addColumn(dbColumn(ColName, Access, TabId, ColId, Type, Offset, Width))

def process(fn):
    db = dbFile(fn)
    
    
    db.dump()
    db.dumpTabDesc()

def main(argv):
    if fDebug:
            pydevd.settrace('localhost', port=2255, stdoutToServer=True, stderrToServer=True,suspend=False)
    for fn in argv[1:]: process(fn)

#print "%08X" % (zlib.adler32("\0"*0, 0xABBABBDB) & 0xFFFFFFFF)
#print "%08X" % ([zlib.adler32("\0"*0xFE0, 0xABBABBDB) & 0xFFFFFFFF)
if __name__=="__main__": main(sys.argv)
