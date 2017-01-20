from ODBC_com import *
import socket

buf1 = "008003a101010080000ce023bd0a00000000000000000000000141646d696e6973747261746f720000000000000000000000000000000000000041646d696e6973747261746f720000000000000000000000000000000000000076617264617461000000000000000000000000000000000000000000000000000000000000000000".decode("HEX")

odbc = ODBC("127.0.0.1",5555)                                                                       #Open connect with target IP and Port.
print odbc.GetRunningBases()
#print odbc.OpenBase("RemoteDefault","D41D8CD98F00B204E9800998ECF8427E","dynamic")
print odbc.OpenBase("Administrator","7B7BC2512EE1FEDCD76BDC68926D4F7B","vardata")                   #Open base with Username, Password and database name. Return tables with ID.
#print odbc.OpenBase("testadmin","12","dynamic")                   #Open base with Username, Password and database name. Return tables with ID.
#print odbc.OpenBase("$SuperUser","D41D8CD98F00B204E9800998ECF8427E".decode("HEX"),"dynamic")
#print odbc.OpenTable("StringTab")                                                                     #Open table with table name. Return result: 0 - OK.
print odbc.OpenTable("SPAMapper")                                                                     #Open table with table name. Return result: 0 - OK.
#print odbc.cur_table.Indexes
#print odbc.SetIndex(1)
#print odbc.SeekFirst()
print odbc.MoveFirst(10)                                                                            #Get records from first. Arg - records count. Return printable dbaPacket.


#soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#soc.connect(("127.0.0.1",5555))
#buf = soc.recv(100)
#print buf.encode("HEX")
#soc.send(buf1)
#buf = soc.recv(2)
#size, = unpack(">H",buf)
#print size
#print buf.encode("HEX")
#buf = ""
#while len(buf) != size:
#    buf += soc.recv(0x1000)
#print dbaPacket(buf)

#odbc = ODBC("127.0.0.1",5555)
#print odbc.GetRunningBases()