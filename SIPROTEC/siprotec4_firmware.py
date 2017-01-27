import struct
from struct import pack, unpack
from os import listdir
from os.path import isfile, join
import sys
import zlib
import os
import re

# Realloc: new_size = 60, old_size = 24, from = 0xb5c4b51c (virtual_file_ex+34c), old_chunk_address = 0xb90b5a70, new_chunk_address = 0xb8da8e30
# Free: size = 73, from = 0xb5c4c640 (tsrm_realpath+130), chunk_ptr address = 0xb8da7280
header_size = 0x104
UNPACK_PATH = ''


class PCK_elem:
    def __init__(self, name, CRC, filesize, data):
        self.fullname = name
        self.name = name[name.rfind(os.sep) + 1:]
        self.CRC = CRC
        self.filesize = filesize
        self.data = data


class PCK_archive:
    def __init__(self, data):
        self.elems = []
        ptr = 0
        datalen = len(data)
        while ptr < datalen:
            header = data[ptr:ptr + header_size]
            ptr = ptr + header_size
            # print len(header)
            elem_name, CRC, filesize = struct.unpack("252sII", header)
            elem_name = elem_name.strip("\0")
            print "name %s, name len %d, CRC 0x%08x, filesize 0x%08x" % (
            elem_name[elem_name.rfind(os.sep) + 1:], len(elem_name), CRC, filesize)
            self.elems.append(PCK_elem(elem_name, CRC, filesize, data[ptr:ptr + filesize]))
            ptr = ptr + filesize
        print len(self.elems)

    def Extract(self, path=""):
        if path == "":
            dir = os.getcwd()
        elif os.path.splitdrive(path)[0] == "":
            dir = os.getcwd() + os.sep + path.strip(os.sep) + os.sep
            if not os.path.exists(dir): os.makedirs(dir)
        else:
            dir = path
        for elem in self.elems:
            f = open(dir + elem.name, "wb+")
            f.write(elem.data)
            f.close()


def process_file(filename):
    global UNPACK_PATH

    with open(filename, 'rb') as f:
        PCK = PCK_archive(f.read())
        UNPACK_PATH = filename + '.unpacked'
        PCK.Extract(UNPACK_PATH)


def parse_kon_file(filename):
    unpack_dir = join(UNPACK_PATH, 'kbin_' + filename)

    try:
        os.makedirs(unpack_dir)
    except Exception as e:
    #     print("Failed to make dir " + unpack_dir + " :(")
    #     print(e)
        pass

    with open(join(UNPACK_PATH, filename), "rb") as f:
        data = f.read()

        kbin_content = re.search('KBIN(.*?)ENDE', data, re.DOTALL)
        if(kbin_content is None):
            print 'Error: unexpected file format:', join(UNPACK_PATH, filename)
            return

        f.seek(4)
        data_count = 0
        end_found = False
        while not end_found:
            try:
                header, size = unpack("4sI", f.read(8))
            except Exception as e:
                print 'Error', e
                break

            # print("Found " + header + " size of " + hex(size))

            if header == "HEAD":
                code_type, minaddr, maxaddr, main, xorcks = unpack("4sIIII", f.read(size))
                print("[*] Found HEAD section")
                print("code type: " + code_type)
                print("min addr: " + hex(minaddr))
                print("max addr: " + hex(maxaddr))
                print("main addr: " + hex(main))
                print("xorcks: " + hex(xorcks))
                print ('%s: 0x%08x:0x%08x' % (code_type, minaddr, maxaddr) + "\n")

            elif header == "INFO":
                unit, device, version, number = unpack("8s8s15sB", f.read(size))
                print("[*] Found INFO section")
                print("Unit: " + unit)
                print("Device: " + device)
                print("Version: " + version)
                print("Number: " + hex(number) + "\n")

            elif header == "TITL":
                t_size = size
                if size > 0x100:
                    t_size = 0x100

                title = unpack(str(t_size) + "s", f.read(t_size))[0].rstrip('\n\x00')
                print("[*] Found TITL section")
                print (title + "\n")
                if t_size != size:
                    f.seek(size - t_size, 1)

            elif header == "DATC":
                print("[*] Found DATC section")
                datc_start_addr, datc_size, crc = unpack("III", f.read(0x0c))
                # print "DATC:"
                print("Start addr: " + hex(datc_start_addr) + " Size: " + hex(datc_size) + " CRC: " + hex(crc) + "\n")
                # print("Size: " + hex(datc_size))
                # print("CRC: " + hex(crc))

                datc = f.read(size - 0x0c)
                datc_fname = unpack_dir + os.sep + "datac_" + hex(data_count)
                with open(datc_fname, "wb") as datac_f:
                    datac_f.write(zlib.decompress(datc))

                data_count += 1

            elif header == "DATA":
                print("[*] Found DATA section")
                start_addr, data_size, crc = unpack("III", f.read(0x0c))

                print("Start addr: " + hex(start_addr))
                print("Size: " + hex(data_size))
                print("CRC: " + hex(crc) + "\n")

                data = f.read(size - 0x0c)
                data_fname = unpack_dir + os.sep + "data_" + hex(data_count)
                with open(data_fname, "wb") as data_f:
                    data_f.write(data)

                data_count += 1

            elif header == "ENDE":
                end_found = True
                f.seek(size, 1)

            else:
                f.seek(size, 1)


    print ('[%s] %s: 0x%08x:0x%08x' % (title, code_type, minaddr, maxaddr))
    #print ('DATC start addr: 0x%08x size:0x%x' % (datc_start_addr, datc_size))


def parse_kon_files():
    unpacked_files = [f for f in listdir(UNPACK_PATH) if isfile(join(UNPACK_PATH, f))]

    for file in unpacked_files:
        if (file[-4:].lower() == '.kon'):
            print '\nParsing', file
            parse_kon_file(file)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Missed filename"
        exit(0)
    for filename in sys.argv[1:]:
        process_file(filename)
        parse_kon_files()
