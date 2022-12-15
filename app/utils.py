import hashlib
import hmac
import random
import string
import urllib.parse
import base64


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


def get_signature(string_to_sign, secret_key):
    new_hmac = hmac.new(secret_key.encode('utf-8'), digestmod=hashlib.sha1)
    new_hmac.update(string_to_sign.encode('utf-8'))
    return urllib.parse.quote(base64.encodebytes(new_hmac.digest()).strip().decode('utf-8')).replace("/", "%2F")


def get_etag(data):
    if type(data) == str:
        data = data.encode("utf-8")
    return hashlib.md5(data).hexdigest()
