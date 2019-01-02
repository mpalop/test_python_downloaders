# Python Script to download multiple files: 

#### Requirements: 
*  Multiple protocols (http, https, ftp, s3)
*  Parallel download of files 
*  Clear scripting and friendliness to add new protocols

#### Assumptions: 
* No proxy usage 
* FTP connections without user:password 
* AWS authentication using AWS Profile (No keys)
* I added a lot of verbosity to understand what is the program doing 

#### Installation:
* Unzip the file into a folder. From this folder, execute:
```bash 
make install 
```    
   
* In case of not having make, use the following commands: 
```bash
virtualenv --python=python3.7 env 
source ./env/bin/activate
pip3 install -r requirements.txt    
```

#### Program design decisions:
* Initially I tried to develop it using [Asyncio](https://docs.python.org/3/library/asyncio.html). 
  Its a great library to write concurrent code, but the rest of libraries must be compatibles with it,
  and this doesn't happens with the s3 requirement (there are some, but are in a very beta stage) 
* I got back to the Multithread module of Python Standard Library. 
* In order to make easy to add new protocols, I encapsuled the download functionalily into Classes 
  that have the same structure (there are no Interface-like thing on Python)

![Schema](images/program_schema.png)


#### Usage:
```bash
➜ source ./env/bin/activate
(env)➜  test ./downloader.py
usage: downloader.py [-h] [--profile PROFILE] [--chunk CHUNK]
                     [--threads THREADS]
                     urls [urls ...]
downloader.py: error: the following arguments are required: urls
```

Example of execution: 
```bash 
python3 downloader.py  s3://mpalop-test-bucket/test3.pdf s3://mpalop-test-bucket/fake.jpg  https://stackoverflow.com/questions/27208131/urllib-cannot-read-https ftp://speedtest:speedtest@ftp.otenet.gr/test100k.db   --profile parsing
```

Results:
```bash
2018-11-10 15:46:11,975 - root - INFO - working with profile: parsing
2018-11-10 15:46:11,976 - __main__ - INFO - s3://mpalop-test-bucket/test3.pdf to the queue
2018-11-10 15:46:11,976 - __main__ - INFO - s3://mpalop-test-bucket/fake.jpg to the queue
2018-11-10 15:46:11,976 - root - INFO - Thread: getting s3://mpalop-test-bucket/test3.pdf
2018-11-10 15:46:11,976 - __main__ - INFO - https://stackoverflow.com/questions/27208131/urllib-cannot-read-https to the queue
2018-11-10 15:46:11,977 - root - INFO - Thread: getting s3://mpalop-test-bucket/fake.jpg
2018-11-10 15:46:11,977 - __main__ - INFO - creating
2018-11-10 15:46:11,977 - __main__ - INFO - ftp://speedtest:speedtest@ftp.otenet.gr/test100k.db to the queue
2018-11-10 15:46:11,977 - __main__ - INFO - creating
2018-11-10 15:46:11,977 - root - INFO - Thread: getting https://stackoverflow.com/questions/27208131/urllib-cannot-read-https
2018-11-10 15:46:11,989 - root - INFO - Thread: getting ftp://speedtest:speedtest@ftp.otenet.gr/test100k.db
2018-11-10 15:46:12,086 - botocore.credentials - INFO - Found credentials in shared credentials file: ~/.aws/credentials
2018-11-10 15:46:12,093 - botocore.credentials - INFO - Found credentials in shared credentials file: ~/.aws/credentials
2018-11-10 15:46:12,180 - __main__ - INFO - Initializing DownloaderS3
2018-11-10 15:46:12,180 - __main__ - INFO - downloading Bucket:mpalop-test-bucket key:fake.jpg
2018-11-10 15:46:12,188 - __main__ - INFO - Initializing DownloaderS3
2018-11-10 15:46:12,188 - __main__ - INFO - downloading Bucket:mpalop-test-bucket key:test3.pdf
2018-11-10 15:46:12,235 - __main__ - INFO - downloading https://stackoverflow.com/questions/27208131/urllib-cannot-read-https
2018-11-10 15:46:12,663 - __main__ - ERROR - Error downloading s3://mpalop-test-bucket/fake.jpg. {'Code': '404', 'Message': 'Not Found'}
2018-11-10 15:46:13,005 - __main__ - INFO - downloading ftp://speedtest:speedtest@ftp.otenet.gr/test100k.db
2018-11-10 15:46:15,282 - root - INFO - File test3.pdf downloaded
2018-11-10 15:46:15,283 - root - INFO - Time taken: 3.3077399730682373

```
