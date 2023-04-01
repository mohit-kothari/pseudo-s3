import xmltodict
from datetime import datetime, timezone
from fastapi import Response
from .utils import (string_to_bytes,
                    get_host_id)

response_dt_fmt = "%Y-%m-%dT%H:%M:%SZ"


def error_response(message, code, status_code=400, extra_args={}):
    result = {"Error": {"Code": code, "Message": message}}
    if extra_args:
        result["Error"].update(extra_args)
    content = xmltodict.unparse(result)
    return Response(content, media_type="application/xml", status_code=status_code)


def success_response(response_dict, status_code=200, headers=None):
    response_dict[list(response_dict.keys())[0]]["@xmlns"] = "http://s3.amazonaws.com/doc/2006-03-01"
    content = xmltodict.unparse(response_dict)
    if headers:
        return Response(content, media_type="application/xml", status_code=status_code, headers=headers)
    return Response(content, media_type="application/xml", status_code=status_code)


def invalid_signature(access_key_id, sign_string, signature, request_id):
    code = "SignatureDoesNotMatch"
    msg = "The request signature we calculated does not match the signature you provided. Check your key and signing method."
    status_code = 400
    extra_args = {
        "AWSAccessKeyId": access_key_id,
        "StringToSign": sign_string,
        "SignatureProvided": signature,
        "StringToSignBytes": string_to_bytes(sign_string),
        "RequestId": request_id,
        "HostId": get_host_id()
    }
    return error_response(msg, code, status_code, extra_args)


def duplicate_bucket_error(bucket_name):
    code = "BucketAlreadyOwnedByYou"
    msg = "Your previous request to create the named bucket succeeded and you already own it."
    status_code = 400
    extra_args = {
        "BucketName": bucket_name
    }
    return error_response(msg, code, status_code, extra_args)


def invalid_location(request_id):
    code = "IllegalLocationConstraintException"
    msg = "The unspecified location constraint is incompatible for the region specific endpoint this request was sent to."
    status_code = 400
    extra_args = {
        "RequestID": request_id
    }
    return error_response(msg, code, status_code, extra_args)

def invalid_key(file_name, request_id):
    code = "NoSuchKey"
    msg = "The specified key does not exist."
    status_code = 404
    extra_args = {
        "Key": file_name,
        "RequestID": request_id
    }
    return error_response(msg, code, status_code, extra_args)




def bucket_not_empty(bucket_name, request_id):
    code = "BucketNotEmpty"
    msg = "The bucket you tried to delete is not empty"
    status_code = 409
    extra_args = {
        "BucketName": bucket_name,
        "RequestId": request_id,
        "HostId": get_host_id()
    }
    return error_response(msg, code, status_code, extra_args)


def request_expired(expiry, request_id):
    code = "AccessDenied"
    msg = "Request has expired"
    status_code = 403
    extra_args = {
        "Expires": expiry.strftime(response_dt_fmt),
        "ServerTime": datetime.now(tz=timezone.utc).strftime(response_dt_fmt),
        "RequestId": request_id,
        "HostId": get_host_id()
    }
    return error_response(msg, code, status_code, extra_args)


def multipart_upload_result(location, bucket, etag, headers=None):
    response_dict = {
        "CompleteMultipartUploadResult": {
            "Location": location,
            "Bucket": bucket,
            "Etag": "&#34;{}&#34;".format(etag)
        }
    }
    return success_response(response_dict, 200, headers)


def multipart_upload_start(bucket, path, upload_id, headers=None):
    response_dict = {
        "InitiateMultipartUploadResult": {
            "Bucket": bucket,
            "Key": path,
            "UploadId": upload_id
        }
    }
    return success_response(response_dict, 200, headers)


def no_content(headers=None):
    return Response("", status_code=204, headers=headers)


def multiple_obj_delete_successful(deleted_files, headers=None):
    response_dict = {
        "DeleteResult": {
            "Deleted": [{"Key": i, "VersionId": None} for i in deleted_files]
        }
    }
    return success_response(response_dict, 200, headers)
