#!/usr/bin/env python3

import base64
import datetime
import json
import xmltodict
from typing import Union, Any
from fastapi import FastAPI, Response, Request, Query, File, Form

from .settings import settings

from . import aws_responses as AWSResponse
from .utils import (
    get_etag, get_signature, get_sha256_signature,
    prepare_sign_string, get_amzn_requestid,
    get_upload_id, get_secret_key
)

exec("from .{} import *".format(settings.model))

# Server Logic
app = FastAPI()


def DashingQuery(default: Any, *, convert_underscores=True, **kwargs) -> Any:
    query = Query(default, **kwargs)
    query.convert_underscores = convert_underscores
    return query


def split_query_params(params):
    def query(q):
        if len(q) == 1:
            q.append(None)
        elif len(q) > 2:
            q = [q[0], "=".join(q[1:])]
        return q
    return dict([query(list(i.split("="))) for i in params.split("&")])


@app.middleware("http")
async def set_region(request: Request, call_next):
    request_id = get_amzn_requestid()
    request.state.request_id = request_id
    authorization = request.headers.get("Authorization", "")
    host = request.headers.get("host", "")
    if "amazonaws.com" in host:
        if len(host.split(".")) == 5:
            bucket = host.split(".")[0]
            request.scope["path"] = "/" + bucket + request.scope["path"]
        request.state.aws_region = host.split(".")[2]
    if authorization and "AWS4-HMAC-SHA256" in authorization:
        authorization_headers = dict([i.split("=") for i in authorization.split(", ")])
        request.state.aws_region = authorization_headers["AWS4-HMAC-SHA256 Credential"].split("/")[2]
        if settings.validate_signature:
            if get_sha256_signature(request, authorization_headers["AWS4-HMAC-SHA256 Credential"]) != authorization_headers["Signature"]:
                return AWSResponse.invalid_signature("", "", "", request.state.request_id)
    # else:
    #     print("!!!!!!!!!!!!!!!!!!!!! not authorization !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1", request.headers)
    #     request.state.aws_region = 'us-east-1'
    response = await call_next(request)
    response.headers['x-amz-request-id'] = request_id
    return response


@app.get("/")
async def list_buckets(request: Request, response: Response):
    bucket_data = S3Region(request.state.aws_region).list_buckets
    return AWSResponse.success_response(bucket_data)


# @app.head("/{bucket_name}")
# async def head_bucket(bucket_name, request, response):
#     bucket = S3Bucket(bucket_name, request.state.aws_region)
#     if bucket.exists:
#         Response("", media_type="binary/octet-stream", headers=headers)
#     else:
#         aws_responses.invalid_key("test", "rest")


@app.get("/{bucket_name}")
async def list_objects(bucket_name: Union[str, None], request: Request, response: Response, 
                       encoding_type: str = DashingQuery(None), list_type: str = DashingQuery(None),
                       versions: str = DashingQuery("no"), marker: str = DashingQuery(None),
                       continuation_token: str = DashingQuery(None), prefix: str = DashingQuery(None),
                       max_keys: int = DashingQuery(1000), delimiter: str = DashingQuery(None)):
    bucket = S3Bucket(bucket_name, request.state.aws_region)
    if not bucket.exists:
        return AWSResponse.invalid_location(request.state.request_id)
    if versions == "no":
        if list_type == "2":
            data = bucket.list_objects_v2(encoding_type, prefix, max_keys, continuation_token, delimiter)
        else:
            data = bucket.list_objects(encoding_type, prefix, max_keys, marker, delimiter)
        return AWSResponse.success_response(data)
    else:
        data = bucket.list_object_versions(encoding_type, prefix, max_keys, marker, delimiter)
        return AWSResponse.success_response(data)
    


@app.put("/{bucket_name}")
async def create_bucket(bucket_name: Union[str, None], request: Request, response: Response):
    body = await request.body()
    body = body.decode('utf-8')
    if body:
        req_data = xmltodict.parse(body)
        region = S3Region(req_data["CreateBucketConfiguration"]["LocationConstraint"])
    else:
        region = request.state.aws_region
    bucket = S3Bucket(bucket_name, region)
    if bucket.exists:
        return AWSResponse.duplicate_bucket_error(bucket_name)
    data = bucket.create()
    if data:
        location = '{scheme}://{name}.s3.{host}:{port}/'.format(name=bucket.name, scheme=request.url.scheme, host=request.url.hostname, port=request.url.port)
        return AWSResponse.success_response(data, headers={"location": location})
    return AWSResponse.duplicate_bucket_error(bucket_name)


@app.delete("/{bucket_name}")
async def delete_bucket(bucket_name: Union[str, None], request: Request, response: Response):
    bucket_object = S3Bucket(bucket_name, request.state.aws_region)
    if not bucket_object.exists:
        return AWSResponse.invalid_location(request.state.request_id)
    if not bucket_object.is_empty:
        return AWSResponse.bucket_not_empty(bucket_name, request.state.request_id)
    if bucket_object.delete():
        return AWSResponse.no_content()


@app.put("/{file_path:path}")
async def create_object(file_path: Union[str, None], request: Request, response: Response, uploadId: str = DashingQuery(None), partNumber: str = DashingQuery(None)):
    body = await request.body()
    bucket, path = S3Object.split_bucket_and_path(file_path)
    obj = S3Object(path, bucket, request.state.aws_region)
    if not obj.bucket.exists:
        return AWSResponse.invalid_location(request.state.request_id)
    if uploadId and partNumber:
        # add part of large file
        etag = obj.create_temp_file(body, uploadId, partNumber)
    else:
        # create object
        try:
            etag = obj.create_object(body.decode('utf-8'))
        except UnicodeDecodeError:
            etag = obj.create_object(body)
        metadata = {}
        for key, val in request.headers.items():
            if key.startswith("x-amz-meta"):
                metadata[key] = val
        if metadata:
            obj.set_metadata(metadata)
    location = '{scheme}://{name}.s3.{host}:{port}/'.format(name=file_path.split("/")[0], scheme=request.url.scheme, host=request.url.hostname, port=request.url.port)
    return Response("", media_type="plain/text", headers={"location": location, "Etag": etag})


@app.head("/{file_path:path}")
async def head_object(file_path: Union[str, None], request: Request, response: Response):
    bucket, path = S3Object.split_bucket_and_path(file_path)
    if path:
        obj = S3Object(path, bucket, request.state.aws_region)
        if not obj.exists:
            return AWSResponse.invalid_key(obj.relative_path, request.state.request_id)
        headers = {'content-length': str(obj.size), "etag": obj.etag, "last-modified": obj.mtime}
        headers.update(obj.get_metadata())
    else:
        bucket = S3Bucket(bucket, request.state.aws_region)
        if not bucket.exists:
            return AWSResponse.invalid_key(bucket.name, request.state.request_id) 
        headers = {}
        
    return Response("", media_type="binary/octet-stream", headers=headers)


@app.get("/{file_path:path}")
async def read_object(file_path: Union[str, None], request: Request, response: Response):
    query_params = split_query_params(request.query_params.__str__())
    bucket, path = S3Object.split_bucket_and_path(file_path)
    aws_region = getattr(request.state, "aws_region", None)

    ###########################################################################
    # This is logic to serve pre_signed urls. Needs to be revisited again
    if query_params.get("Expires", None):
        secret_key = get_secret_key(query_params.get("AWSAccessKeyId"))
        if secret_key:
            string_to_sign = prepare_sign_string("GET", "/"+file_path, query_params["Expires"])
            if not aws_region:
                aws_region = S3Obj.buckets.get(bucket, None)
            if (not settings.validate_signature or (query_params.get("Signature", None) == get_signature(string_to_sign, secret_key))) and aws_region:
                expiry = datetime.datetime.fromtimestamp(int(query_params["Expires"]), datetime.timezone.utc)
                if expiry < datetime.datetime.now(tz=datetime.timezone.utc):
                    return AWSResponse.request_expired(expiry, request.state.request_id)
            else:
                return AWSResponse.invalid_signature(query_params["AWSAccessKeyId"], string_to_sign, query_params["Signature"], request.state.request_id)
    ###########################################################################
    status_code = 200
    obj = S3Object(path, bucket, aws_region)
    if not obj.exists:
        return AWSResponse.invalid_key(obj.relative_path, request.state.request_id)
    if request.headers.get("If-Modified-Since", None):
        date = datetime.datetime.strptime(request.headers["If-Modified-Since"], "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=datetime.timezone.utc)
        if date > datetime.datetime.fromtimestamp(obj.stats.st_mtime, tz=datetime.timezone.utc):
            raise 304

    range_low = range_high = content_range = None
    if request.headers.get("Range", None):
        range_low, range_high = request.headers["Range"].split("=")[1].split("-")
        range_low = int(range_low) if range_low else None 
        range_high = int(range_high) if range_high else None
        content_range = "bytes {0}-{1}/{2}".format(range_low, range_high, obj.size)
        status_code = 206
    resp = obj.read_object(range_low, range_high)
    etag = get_etag(resp)
    headers = {
        'content-length': str(len(resp)),
        "etag": '"{}"'.format(etag),
        "last-modified": obj.mtime}
    if content_range:
        headers["content-range"] = content_range
    else:
        headers['accept-ranges'] = 'bytes'

    headers.update(obj.get_metadata())
    try:
        resp = resp.decode('utf-8')
    except UnicodeDecodeError:
        pass
    return Response(resp, media_type="binary/octet-stream", headers=headers, status_code=status_code)


@app.post("/{file_path:path}")
async def post(file_path: Union[str, None], request: Request, response: Response, uploadId: str = DashingQuery(None), key: str = Form(None), 
               AWSAccessKeyId = Form(None), signature = Form(None), policy = Form(None), file=File(None)):
    bucket, path = S3Object.split_bucket_and_path(file_path)
    location = '{scheme}://{name}.s3.{host}:{port}{path}'.format(name=bucket, scheme=request.url.scheme, host=request.url.hostname, port=request.url.port, path=file_path)
    if getattr(request.state, "aws_region", None):
        bucket = S3Bucket(bucket, request.state.aws_region)
        if not bucket.exists:
            return AWSResponse.invalid_location(request.state.request_id)
        if uploadId:
            # large file upload finish
            body = await request.body()
            request_data = xmltodict.parse(body)
            obj = S3Object(path, bucket, request.state.aws_region)
            obj.merge_temp_file(uploadId, request_data["CompleteMultipartUpload"]["Part"])
            bucket.meta_manager.move(uploadId, path)
            return AWSResponse.multipart_upload_result(location, bucket, obj.etag, {"location": location})
        elif request.query_params.__str__() == "delete=":
            # Delete multiple objects
            body = await request.body()
            request_data = xmltodict.parse(body)
            files_to_delete = [i["Key"] for i in request_data["Delete"]["Object"]]
            for file in files_to_delete:
                S3Object(file, bucket, request.state.aws_region).delete_object()
            return AWSResponse.multiple_obj_delete_successful(files_to_delete)
        elif request.query_params.__str__() == "uploads=":
            # Start large file upload 
            upload_id = get_upload_id()
            metadata = {}
            for key, val in request.headers.items():
                if key.startswith("x-amz-meta"):
                    metadata[key] = val
            if metadata:
                bucket.meta_manager.set(upload_id, metadata)
            return AWSResponse.multipart_upload_start(bucket, path, upload_id, {"location": location})
    elif file:
        # handle presigned post url
        secret_key = get_secret_key(AWSAccessKeyId)
        if secret_key:
            if settings.validate_signature:
                server_signature = get_signature(policy, secret_key, url_encoded=False)
                if signature != server_signature:
                    return AWSResponse.invalid_signature(AWSAccessKeyId, policy, signature, request.state.request_id)
            policy = json.loads(base64.b64decode(policy))
            expiry = datetime.datetime.strptime(policy["expiration"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
            if expiry < datetime.datetime.now(tz=datetime.timezone.utc):
                return AWSResponse.request_expired(expiry, request.state.request_id)
            aws_region = S3Obj.buckets.get(bucket, None)
            bucket = S3Bucket(bucket, aws_region)
            if not bucket.exists:
                return AWSResponse.invalid_location(request.state.request_id)
            obj = S3Object(key, bucket, aws_region)
            body = await file.read()
            try:
                etag = obj.create_object(body.decode('utf-8'))
            except UnicodeDecodeError:
                etag = obj.create_object(body)
            return AWSResponse.no_content()


@app.delete("/{file_path:path}")
async def delete_object(file_path: Union[str, None], request: Request, response: Response):
    bucket, path = S3Object.split_bucket_and_path(file_path)
    obj = S3Object(path, bucket, request.state.aws_region)
    obj.delete_object()
    return AWSResponse.no_content()

if __name__ == '__main__':
    app.run()
