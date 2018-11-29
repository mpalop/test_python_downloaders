#!/usr/bin/env python3

import os
import argparse
import logging
from time import time

import boto3
import botocore

import urllib
from urllib.parse import urlparse

from queue import Queue
from threading import ThreadError, Thread

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#
# Classes that perform the Download. As Python lacks of Interface concept, both must work in the same way
# Every new downloading class must comply with this format:
# Init method that loads the necessary parameters
# Static method create that builds the object properly
# Download method with the URL to download as parameter
#
class DownloaderHTML(object):
    def __init__(self, chunk):
        """
        Init method
        :param chunk: size in bytes of the parts of the file to download
        """
        self.chunk = chunk

    @staticmethod
    def create(chunk):
        return DownloaderHTML(chunk)

    def download(self, url):
        req = urllib.request.Request(url)
        res = ""
        try:
            res = urllib.request.urlopen(req)
        except urllib.error.URLError as e:
            logger.error("Error downloading {}. {}".format(url, str(e.reason)))
            return False

        logger.info("downloading {}".format(url))
        filename = os.path.basename(url)
        with open(filename, 'wb') as f_save:
            while True:
                chunk = res.read(self.chunk)
                if not chunk:
                    break
                f_save.write(chunk)
        return True


class DownloaderS3(object):

    def __init__(self, profile):
        """
        Init method
        :param profile: AWS Profile to work with
        """
        self.profile = profile
        if self.profile:
            boto3.setup_default_session(profile_name=profile)
        self.s3 = boto3.resource("s3")
        logger.info("Initializing DownloaderS3")

    @staticmethod
    def create(profile=""):
        logger.info("creating")
        return DownloaderS3(profile)

    def download(self, url):
        filename = os.path.basename(url)
        parsed_url = urlparse(url)
        bucket=parsed_url.netloc
        key=parsed_url.path[1:]
        logger.info("downloading Bucket:{} key:{}".format(bucket, key))
        try:
            self.s3.Bucket(bucket).download_file(key, filename)
            logging.info("File {} downloaded".format(filename))
            return True
        except botocore.exceptions.ClientError as e:
            logger.error("Error downloading {}. {}".format(url, str(e.response['Error'])))
            return False


# The Map that associates protocols with Classes and its parameters
URL_MAP = { "s3":   {"Class": DownloaderS3, "Parameters": ["profile"]},
            "http": {"Class": DownloaderHTML, "Parameters": ["chunk"]},
            "https": {"Class": DownloaderHTML, "Parameters": ["chunk"]},
            "ftp": {"Class": DownloaderHTML, "Parameters": ["chunk"]}
          }

# The downloader Class (Thread)
class DownloadWorker(Thread):
    def __init__(self, queue, global_parameters):
        Thread.__init__(self)
        self.queue = queue
        self.global_parameters = global_parameters

    def _get_real_parameters(self, params_list):
        """
        Auxiliar method to pick the right parameters for the downloader
        :param params_list:
        :return:
        """
        real_parameters = [x for x in self.global_parameters.keys() if x in params_list]
        parameters = {k: v for k, v in self.global_parameters.items() if k in real_parameters}
        return parameters

    def run(self):
        while True:
            url = self.queue.get()
            logging.info("Thread: getting {}".format(url))
            scheme = urlparse(url).scheme
            try:
                downloader_def = URL_MAP.get(scheme, "")
                if not downloader_def:
                    logging.error("Thread: Downloading {}: No Implemented Downloader to handle the protocol {}".format(url, scheme))
                else:
                    # this is for pick the global parameters that each particular downloader needs
                    real_parameters = self._get_real_parameters(downloader_def['Parameters'])

                    # create the downloader with proper parameters
                    downloader = downloader_def['Class'].create(**real_parameters)

                    # perform the download
                    ret = downloader.download(url)
            finally:
                self.queue.task_done()


def process(args):
    start_time = time()
    queue = Queue()
    for w in range(args.threads):
        worker = DownloadWorker(queue=queue, global_parameters={"profile": args.profile, "chunk": args.chunk })
        worker.daemon = True
        worker.start()

    for url in args.urls:
        logger.info("{} to the queue".format(url))
        queue.put(url)

    queue.join()
    logging.info("Time taken: {}".format(time()-start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="This program download files")
    parser.add_argument("--profile", help="AWS profile to use", default="default")
    parser.add_argument("--chunk", help="size of file chunks (http(s) and ftp download)", default=1024)
    parser.add_argument("--threads", help="number of threads to run", default=8)
    parser.add_argument("urls", help="list of urls to download", nargs="+")

    args = parser.parse_args()
    if args.profile:
        logging.info("working with profile: {0}".format(args.profile))

    process(args)




