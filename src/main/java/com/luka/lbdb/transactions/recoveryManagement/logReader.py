import sys, struct

m = {1:"QUIESCENT_CHECKPOINT",2:"START",3:"COMMIT",4:"ROLLBACK",
     5:"SETINT",6:"SETSTRING",7:"SETBOOLEAN",8:"APPEND"}

BLK = 4096
MAX_BYTES_PER_CHAR = 4

def read_int(b, pos):
    return struct.unpack_from(">I", b, pos)[0]

def read_string_slot(b, pos, length):
    s = b[pos:pos+length].decode("utf-8", errors="ignore")
    s = s.rstrip("\x00").replace("\n","").replace("\r","")
    return s, pos+length


def parse_record(data):
    t = read_int(data, 0)
    idx = 4
    out = [m.get(t, str(t))]

    if t in (2,3,4):
        tx = read_int(data, idx)
        out.append(str(tx))
        return "<" + " ".join(out) + ">"

    if t == 1:
        last_tx = read_int(data, idx)
        out.append(str(last_tx))
        return "<" + " ".join(out) + ">"

    if t in (5,7):
        tx = read_int(data, idx)
        idx += 4
        remaining = len(data) - idx
        filename_slot = remaining - 4*4
        fname, idx = read_string_slot(data, idx, filename_slot)
        blockId = read_int(data, idx)
        idx += 4
        offset = read_int(data, idx)
        idx += 4
        oldVal = read_int(data, idx)
        idx += 4
        newVal = read_int(data, idx)
        out.extend([str(tx), fname, str(blockId), str(offset), str(oldVal), str(newVal)])
        return "<" + " ".join(out) + ">"

    if t == 6:
        tx = read_int(data, 4)
        idx = 8

        total_len = len(data)
        remaining_after_filename = total_len - idx
        slot_len = (remaining_after_filename - 8) // 3

        fname, idx = read_string_slot(data, idx, slot_len)
        blockId = read_int(data, idx)
        idx += 4
        offset = read_int(data, idx)
        idx += 4
        oldVal, idx = read_string_slot(data, idx, slot_len)
        newVal, idx = read_string_slot(data, idx, slot_len)

        out = [m[t], str(tx), fname, str(blockId), str(offset), oldVal, newVal]
        return "<" + " ".join(out) + ">"


    if t == 8:
        tx = read_int(data, idx)
        idx += 4
        remaining = len(data) - idx
        fname, idx = read_string_slot(data, idx, remaining)
        out.extend([str(tx), fname])
        return "<" + " ".join(out) + ">"

    return "<UNKNOWN {}>".format(t)

with open(sys.argv[1], "rb") as f:
    f.seek(0,2)
    filesize = f.tell()
    blocks = filesize // BLK

    for b in range(blocks-1, -1, -1):
        f.seek(b*BLK)
        block = f.read(BLK)

        boundary = read_int(block, 0)
        pos = boundary
        records = []

        while pos < BLK:
            if pos + 4 > BLK:
                break
            ln = read_int(block, pos)
            if ln <= 0 or pos + 4 + ln > BLK:
                break
            data = block[pos+4 : pos+4+ln]
            records.append(data)
            pos += 4 + ln

        for rec in reversed(records):
            print(parse_record(rec))
