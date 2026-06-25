"""零依赖 TensorBoard tfevents 标量读取器。

训练侧(SpecForge `--report-to tensorboard`,leader rank-0)写 `*.tfevents.*`;
Hearth 只读解析,不在训练节点装任何依赖(tbparse/tensorboard 都不需要)。

格式:
- TFRecord 帧:`[u64 len LE][u32 crc][payload(len)][u32 crc]`(crc 不校验)。
- payload = `tensorflow.Event` proto。需要的字段:
    field1 wall_time(double, wire1)、field2 step(int64, wire0)、field5 summary(message, wire2)。
- Summary.field1 value(repeated Summary.Value):
    Value.field1 tag(string)、field2 simple_value(float, wire5, 旧式)、
    field8 tensor(TensorProto, 新式 torch/HF SummaryWriter)。
- TensorProto 标量:field4 tensor_content(4B LE float)或 field5 float_val(repeated float)。
"""
import struct


def _varint(buf, i):
    shift = result = 0
    while True:
        b = buf[i]; i += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, i
        shift += 7


def _fields(buf):
    """逐字段产出 (field_number, wire_type, value_bytes_or_int)。"""
    i, end = 0, len(buf)
    while i < end:
        key, i = _varint(buf, i)
        fn, wt = key >> 3, key & 7
        if wt == 0:
            v, i = _varint(buf, i); yield fn, wt, v
        elif wt == 1:
            yield fn, wt, buf[i:i + 8]; i += 8
        elif wt == 2:
            ln, i = _varint(buf, i); yield fn, wt, buf[i:i + ln]; i += ln
        elif wt == 5:
            yield fn, wt, buf[i:i + 4]; i += 4
        else:
            return  # group/deprecated wire types — stop


def _tensor_float(buf):
    val = None
    for fn, wt, v in _fields(buf):
        if fn == 4 and wt == 2 and len(v) >= 4:        # tensor_content
            val = struct.unpack('<f', v[:4])[0]
        elif fn == 5 and wt == 5:                       # float_val (single)
            val = struct.unpack('<f', v)[0]
        elif fn == 5 and wt == 2 and len(v) >= 4:       # float_val (packed)
            val = struct.unpack('<f', v[:4])[0]
    return val


def _summary_scalars(buf):
    out = []
    for fn, wt, v in _fields(buf):
        if fn == 1 and wt == 2:                         # repeated Summary.Value
            tag = val = None
            for vfn, vwt, vv in _fields(v):
                if vfn == 1 and vwt == 2:
                    tag = vv.decode('utf-8', 'replace')
                elif vfn == 2 and vwt == 5:             # simple_value
                    val = struct.unpack('<f', vv)[0]
                elif vfn == 8 and vwt == 2:             # tensor (modern)
                    val = _tensor_float(vv)
            if tag is not None and val is not None:
                out.append((tag, val))
    return out


def parse_scalars(raw):
    """tfevents 原始字节 → [(tag, step, wall_time, value), ...](按出现顺序)。"""
    out, i, n = [], 0, len(raw)
    while i + 12 <= n:
        ln = struct.unpack('<Q', raw[i:i + 8])[0]; i += 8
        i += 4                                          # length crc
        if i + ln + 4 > n:
            break
        payload = raw[i:i + ln]; i += ln + 4            # payload + data crc
        wall, step, summ = 0.0, 0, None
        for fn, wt, v in _fields(payload):
            if fn == 1 and wt == 1:
                wall = struct.unpack('<d', v)[0]
            elif fn == 2 and wt == 0:
                step = v
            elif fn == 5 and wt == 2:
                summ = v
        if summ is not None:
            for tag, val in _summary_scalars(summ):
                out.append((tag, step, wall, val))
    return out


def series(raw):
    """tfevents → {tag: [(step, wall_time, value), ...]}(各 tag 按 step 升序)。"""
    by = {}
    for tag, step, wall, val in parse_scalars(raw):
        by.setdefault(tag, []).append((step, wall, val))
    for tag in by:
        by[tag].sort(key=lambda x: x[0])
    return by
