# pseudo-s3 - A fast and lightweight AWS S3 Clone
pseudo-s3 is a simple and easy to use AWS S3 clone that allows you to store the files locally and supports most S3 API's.


# Usage

## Python
```
# Install requirements
pip3 install -r requirements.txt

# Start server
BUCKET_PATH=<path to use for storage> uvicorn app.main:app --reload
```
Note: If not path is provided it will create directory named "buckets" in current directory

## Docker
```
docker run --name s3 --rm \
    -v <path to storage>:/buckets \
    -e AWS_ACCESS_KEY=<access key to allow> \
    -e AWS_SECRET_KEY=<secret key to allow> \
    -p 8000:80 \
    mohitkothari/pseudo-s3:latest
```
Note: By default Access key/Secret key will be `pseudoS3AccessKey/pseudoS3SecretKey`


# Contributing
Contributions are welcome! If you have any feature requests or find any bugs, please open an issue or submit a pull request.
