import datetime
import hashlib
import json
import os
import shutil

from app.settings import settings
from app.utils import get_etag


DISPLAY_NAME = settings.name


def set_directory_path(path):
    if not path.endswith("/"):
        return path+"/"
    return path


class S3:
    def __init__(self):
        self.root = set_directory_path(settings.data_root)
        if not os.path.exists(self.root):
            os.makedirs(self.root)
        self.buckets = {}
        for region in os.listdir(self.root):
            for bucket in os.listdir(os.path.join(self.root, region)):
                self.buckets[bucket] = region

S3Obj = S3()


class S3Region:
    def __init__(self, region):
        self.name = region
        self.parent = S3()
        self.path = set_directory_path(os.path.join(self.parent.root, region))
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)

    def __str__(self):
        return self.name

    @property
    def list_buckets(self):
        data = {
            "ListAllMyBucketsResult": {
                "Owner": {
                    "ID": settings.owner_id,
                    "DisplayName": DISPLAY_NAME,
                },
                "Buckets": {"Bucket": [S3Bucket(b, self).__dict__() for b in os.listdir(self.path)]}
            }
        }
        return data


class S3Bucket:
    def __init__(self, name, region):
        self.name = name
        self.region = region if isinstance(region, S3Region) else S3Region(region)
        self.path = set_directory_path(os.path.join(self.region.path, name))
        if self.exists:
            self.meta_manager = MetaManager(self, self.region)
        else:
            self.meta_manager = None

    def __str__(self):
        return self.name

    def __dict__(self):
        return {"Name": self.name, "CreationDate": self.ctime}

    @property
    def ctime(self):
        return datetime.datetime.fromtimestamp(os.path.getctime(self.path), tz=datetime.timezone.utc).strftime(settings.date_fmt)

    @property
    def exists(self):
        return os.path.exists(self.path)

    @property
    def is_empty(self):
        return len([i for i in os.listdir(self.path) if i not in [".metadata.json", ".tmp"]]) == 0

    def create(self):
        if not (self.exists or S3Obj.buckets.get(self.name, None)):
            os.makedirs(self.path)
            S3Obj.buckets[self.name] = self.region.name
            return {"CreateBucketResponse": {"CreateBucketResponse": {"Bucket": self.name}}}
        return False

    def get_obj_list(self):
        file_list = [i for i in list(os.walk(self.path)) if i[2]]
        objects = [S3Object(os.path.join(j[0], k).replace(self.path, ""), self, self.region) for j in file_list for k in j[2] if not (k==".metadata.json" or j[0].endswith(".tmp"))]
        objects.sort(key=lambda x: x.relative_path)
        return objects

    def _apply_marker(self, objects, marker=None):
        if not marker:
            return objects
        marker_obj = next((i for i in objects if i.relative_path == marker), None)
        if marker_obj:
            return objects[objects.index(marker_obj)+1:]
        else:
            return []

    def _check_prefix(self, objects, prefix=None):
        if not prefix:
            return objects
        return [i for i in objects if i.relative_path.startswith(prefix)]

    def _check_delimiter(self, objects, delimiter=None, prefix=None):
        common_prefixes = []
        if not delimiter:
            return objects, common_prefixes
        new_objects = []
        for obj in objects:
            if delimiter in obj.relative_path:
                if prefix:
                    pfx = "{0}{1}{2}".format(prefix, obj.relative_path.replace(prefix, "").split(delimiter)[0], delimiter)
                else:
                    pfx = "{0}{1}".format(obj.relative_path.split(delimiter)[0], delimiter)
                if not pfx in common_prefixes:
                    common_prefixes.append(pfx)
            else:
                new_objects.append(obj)
        return new_objects, common_prefixes

    def _apply_max_keys_limit(self, objects, max_keys=1000):
        istruncated = False
        if len(objects) > max_keys:
            objects = objects[:max_keys]
            istruncated = True
        return objects, istruncated

    def delete(self):
        if os.path.exists(self.meta_manager.metafile):
            os.remove(self.meta_manager.metafile)
        temp_dir = os.path.join(self.path, ".tmp")
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
        os.rmdir(self.path)
        del S3Obj.buckets[self.name]
        return True

    def list_objects(self, encoding_type, prefix=None, max_keys=1000, marker=None, delimiter=None):
        objects = self.get_obj_list()
        objects = self._apply_marker(objects, marker)
        objects = self._check_prefix(objects, prefix)
        objects, common_prefixes = self._check_delimiter(objects, delimiter, prefix)
        objects, istruncated = self._apply_max_keys_limit(objects, max_keys)

        data = {
            "ListBucketResult": {
                "Name": self.name,
                "MaxKeys": max_keys,
                "EncodingType": encoding_type,
                "IsTruncated": istruncated,
                "Marker": marker,
                "Contents": [obj.__dict__() for obj in objects],
                "CommonPrefixes": [{"Prefix": x} for x in common_prefixes]
            }
        }
        if istruncated:
            data["ListBucketResult"]["NextMarker"] = objects[-1].relative_path
        if prefix:
            data["ListBucketResult"]["Prefix"] = prefix
        if delimiter:
            data["ListBucketResult"]["Delimiter"] = delimiter 
        return data

    def list_objects_v2(self, encoding_type, prefix=None, max_keys=1000, marker=None, delimiter=None):
        objects = self.get_obj_list()
        objects = self._apply_marker(objects, marker)
        objects = self._check_prefix(objects, prefix)
        objects, common_prefixes = self._check_delimiter(objects, delimiter, prefix)
        objects, istruncated = self._apply_max_keys_limit(objects, max_keys)

        data = {
            "ListBucketResult": {
                "Name": self.name,
                "Prefix": prefix,
                "MaxKeys": max_keys,
                "KeyCount": len(objects),
                "EncodingType": encoding_type,
                "IsTruncated": istruncated,
                "Contents": [obj.__dict__(v2=True) for obj in objects]
            }
        }
        if istruncated:
            data["ListBucketResult"]["NextContinuationToken"] = objects[-1].relative_path
        return data


class S3Object:

    def __init__(self, relative_path, bucket, region):
        self.relative_path = relative_path
        self.region = region if isinstance(region, S3Region) else S3Region(region)
        self.bucket = bucket if isinstance(bucket, S3Bucket) else S3Bucket(bucket, region)
        self.path = os.path.join(self.bucket.path, relative_path)
        self.exists = False
        if os.path.exists(self.path):
            self.exists = True

    def __dict__(self, v2=None):
        if self.exists:
            data = {
                "Key": self.relative_path,
                "LastModified": self.mtime,
                "ETag": "\"{}\"".format(self.etag),
                "Size": self.size,
                "StorageClass": "STANDARD",
            }
            if not v2:
                data["Owner"] = {"ID": settings.owner_id, "DisplayName": DISPLAY_NAME}
            return data
        else:
            return {}

    @staticmethod
    def split_bucket_and_path(path):
        path = path.split("/")
        return path[0], "/".join(path[1:])

    def __str__(self):
        return self.relative_path

    @property
    def stats(self):
        if self.exists:
            return os.stat(self.path)
        return None

    @property
    def mtime(self):
        stats = self.stats
        return datetime.datetime.fromtimestamp(stats.st_mtime, tz=datetime.timezone.utc).strftime(settings.date_fmt) if stats else ""

    @property
    def ctime(self):
        stats = self.stats
        return datetime.datetime.fromtimestamp(stats.st_ctime, tz=datetime.timezone.utc).strftime(settings.date_fmt) if stats else ""

    @property
    def size(self):
        stats = self.stats
        if stats:
            return stats.st_size
        return 0

    @property
    def etag(self):
        with open(self.path, "rb") as f:
            file_hash = hashlib.md5()
            chunk = f.read(8192)
            while chunk:
                file_hash.update(chunk)
                chunk = f.read(8192)

        return file_hash.hexdigest()

    def create_object(self, data):
        if not self.bucket.exists:
            raise ValueError("Invalid Bucket")
        if not os.path.exists(os.path.dirname(self.path)):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
        mode = "w"
        if type(data) == bytes:
            mode = "wb"
        with open(self.path, mode) as fp:
            fp.write(data)
        return get_etag(data)

    def create_temp_file(self, data, upload_id, part_no):
        if not self.bucket.exists:
            raise ValueError("Invalid Bucket")
        temp_dir = os.path.join(self.bucket.path, ".tmp/{}".format(upload_id))
        temp_file = os.path.join(temp_dir, part_no)
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        with open(temp_file, "wb") as fp:
            fp.write(data)
        return get_etag(data)

    def merge_temp_file(self, upload_id, parts_list):
        temp_dir = os.path.join(self.bucket.path, ".tmp/{}".format(upload_id))
        with open(self.path, "ab") as fp:
            for part in parts_list:
                with open(os.path.join(temp_dir, part["PartNumber"]), "rb") as part:
                    data = part.read()
                    # if hashlib.md5(data).hexdigest() != part["ETag"]:
                    #     raise ValueError("Corrupt file.")
                    fp.write(data)
        shutil.rmtree(temp_dir)
        return True

    def read_object(self, range_low=None, range_high=None):
        if self.exists:
            with open(self.path, "rb") as fp:
                if range_low:
                    fp.seek(range_low, 0)
                    if range_high:
                        return fp.read(range_high-range_low)
                return fp.read()
        else:
            return ""

    def delete_object(self):
        if self.exists:
            os.remove(self.path)
            # self.bucket.meta_manager.delete(self.relative_path)
            return True
        return False

    def set_metadata(self, metadata):
        self.bucket.meta_manager.set(self.relative_path, metadata)

    def get_metadata(self):
        return self.bucket.meta_manager.get(self.relative_path)


class MetaManager:
    def __init__(self, bucket, region):
        self.region = region
        self.bucket = bucket
        self.metafile = os.path.join(self.bucket.path, ".metadata.json")
        if not os.path.exists(self.metafile):
            open(self.metafile, "w+").close()

    def _read(self):
        try:
            with open(self.metafile, "r") as fp:
                return json.load(fp)
        except json.decoder.JSONDecodeError:
            return {}

    def _write(self, data):
        with open(self.metafile, "w") as fp:
            json.dump(data, fp)

    def get(self, object_name):
        return self._read().get(object_name, {})

    def set(self, object_name, meta):
        data = self._read()
        data[object_name] = meta
        self._write(data)

    def delete(self, object_name):
        data = self._read()
        if data.get(object_name, None):
            del data[object_name]
        self._write(data)

    def move(self, old_object, new_object):
        data = self._read()
        if data.get(old_object, None):
            data[new_object] = data[old_object]
            del data[old_object]
        self._write(data)
