# Regenerates python_cases.json: python3 gen_python_cases.py > python_cases.json
import json
cases = [
 r'{"b":1,"a":[1.10,2e3,-0,0.0,1E-7,123456789012345678901234567890,1.5e300,0.1,100.0,1e16,1e15,12345.6789e-10,-2.5]}',
 r'{"s":"café \/ \"q\" \\ \n\t\r\b\f \u0001 \u001f \u007f   😀 <>&","k":null,"t":true,"f":false}',
 r'{"z":{"y":1,"x":[{"b":2,"a":1}]},"a":"é","A":"x","_":"u","ä":1,"aa":2}',
 r'{"dup":1,"other":2,"dup":3}',
 r'[]',
 r'[1e-400,-1e-400,1e400,-1e999,0e5,-0.0]',
 r'{"id":"C1","_modified_at":"2026-01-01T00:00:00.000Z","n":-1.0e-5,"m":5e-324,"big":1.7976931348623157e308}',
]
out = []
for c in cases:
    v = json.loads(c)
    out.append([c, json.dumps(v, separators=(",", ":"), ensure_ascii=False), json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)])
print(json.dumps(out, ensure_ascii=False, indent=1))
