import base64
import hashlib
import hmac
import random
import string
import urllib.parse
from .settings import settings

SIGNED_HEADERS_BLACKLIST = [
    'accept-encoding',
    'amz-sdk-invocation-id',
    'amz-sdk-request',
    'authorization',
    'content-length',
    'expect',
    'user-agent',
    'x-amzn-trace-id',
    'x-forwarded-for',
    'x-real-ip'
]

def get_amzn_requestid():
    res = ''.join(random.choices(string.ascii_uppercase +
                                 string.digits, k=39))
    return base64.b64encode(res.encode()).decode().replace("+", "0").replace("/", "0")[:52]


def get_host_id():
    res = ''.join(random.choices(string.ascii_uppercase +
                                 string.digits, k=56))
    return base64.b64encode(res.encode()).decode().replace("+", "0").replace("/", "0")[:52]


def get_upload_id():
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=57))


def string_to_bytes(string):
    return " ".join(["{:X}".format(ord(i)) for i in string])


def prepare_sign_string(method, file_path, expiry):
    string_to_sign = "{0}\n\n\n{1}\n{2}".format(method.upper(), expiry, file_path)
    return string_to_sign


def get_signature(string_to_sign, secret_key, url_encoded=True):
    new_hmac = hmac.new(secret_key.encode('utf-8'), digestmod=hashlib.sha1)
    new_hmac.update(string_to_sign.encode('utf-8'))
    signature = base64.encodebytes(new_hmac.digest()).strip().decode('utf-8')
    if url_encoded:
        return urllib.parse.quote(signature).replace("/", "%2F")
    return signature


def get_etag(data):
    if type(data) == str:
        data = data.encode("utf-8")
    return hashlib.md5(data).hexdigest()

def _sign(key, msg, hex=False):
    hash = hmac.new(key, msg.encode('utf-8'), hashlib.sha256)
    if hex:
        return hash.hexdigest()
    else:
        return hash.digest()

def canonical_query_string(request):
    query_params = []
    for key, value in request.query_params.items():
        query_params.append(
            (urllib.parse.quote(key, safe='-_.~'), urllib.parse.quote(str(value), safe='-_.~'))
        )
    return "&".join(f"{k}={v}" for k, v in sorted(query_params))

def canonical_headers(request):
    header_map = {}
    for name, value in request.headers.items():
        lname = name.lower()
        if lname not in SIGNED_HEADERS_BLACKLIST:
            if header_map.get("lname", None):
                header_map[lname].append(value)
            else:
                header_map[lname] = [value]

    headers = []
    for key in sorted(set(header_map.keys())):
        value = ','.join(
            ' '.join(v.split()) for v in header_map[key]
        )
        headers.append(f'{key}:{value}')
    return '\n'.join(headers), ";".join(sorted(n.lower().strip() for n in set(header_map)))


def canonical_request(request):
    cr = [request.method.upper()]
    path = request.scope["path"]
    cr.append(path)
    cr.append(canonical_query_string(request))
    headers, signed_headers = canonical_headers(request)
    cr.append(headers + '\n')
    cr.append(signed_headers)
    body_checksum = ""
    if 'X-Amz-Content-SHA256' in request.headers:
        body_checksum = request.headers['X-Amz-Content-SHA256']
    cr.append(body_checksum)
    return hashlib.sha256('\n'.join(cr).encode('utf-8')).hexdigest()

def get_secret_key(access_key):
    secrets = next((i for i in settings.valid_credentials if i["access_key_id"] == access_key), None)
    return secrets["secret_key"] if secrets else None


def get_sha256_signature(request, auth_header):
    canonical = canonical_request(request)
    access_key, timestamp, region, service, request_type = auth_header.split("/")
    secret = f"{timestamp}/{region}/{service}/{request_type}"
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", request.headers["x-amz-date"], secret, canonical])
    secret_key = get_secret_key(access_key)
    k_date = _sign(f"AWS4{secret_key}".encode(), timestamp)   
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, request_type)
    return _sign(k_signing, string_to_sign, hex=True)
