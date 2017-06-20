import boto3
import logging
import json
import os
from datetime import date

#logging.basicConfig(level=logging.INFO, filename="backup.log", format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')

class BackupAgent():

	def __init__(self, settings):
		self.s = settings
	
	def perform_backup(self):
		success = True
		settings = self.s
		logging.info("Backup agent ID: {id}".format(id=settings["id"]))
		logging.info("S3 bucket ID: {id}".format(id=settings["bucket"]))
		logging.info("Number of folders to backup: {count}".format(count=len(settings["folders"])))
		d = date.today().strftime("%Y%m%d")
		logging.info("Folder in S3 bucket: {id}/{date}".format(date=d, id=settings["id"]))
		try:
			self.create_s3_session(settings["aws_profile"])
			self.create_s3_subfolder(settings["bucket"], settings["id"], d)
			for folder in settings["folders"]:
				logging.info("Working on folder: {f}".format(f=folder))
				if self.sync_local_to_s3(settings["bucket"], settings["id"], d, folder) == False:
					success = False
		except e:
			logging.error("Exception during backup process {0}".format(e))
		return success
	
	def create_s3_session(self, profile):
		logging.info("Creating AWS S3 client for profile: {p}".format(p=profile))
		self.aws_session = boto3.Session(profile_name=profile, region_name="ap-southeast-2")
		self.s3client = self.aws_session.resource("s3")
	
	def create_s3_subfolder(self, bucket, agentid, d):
		b = self.s3client.Bucket(bucket)
		key = "{a}/{d}/backup".format(b=bucket, a=agentid, d=d)
		logging.info("Checking if folder {f} exists...".format(f=key))
		objs = list(b.objects.filter(Prefix=key))
		if len(objs) > 0 and objs[0].key == key:
			logging.info("Folder exists already")
		else:
			logging.info("Folder does not exist, creating...")
			b.put_object(Key=key)
	
	def sync_local_to_s3(self, bucket, agentid, d, localdir):
		success = True
		b = self.s3client.Bucket(bucket)
		logging.info("Scanning '{dir}'".format(dir=localdir))
		destination = "{a}/{d}{l}".format(a=agentid, d=d, l=localdir)
		for root, dirs, files in os.walk(localdir, followlinks=True):
			for filename in files:
				try:
					local_path = os.path.join(root, filename)
					rel_path = os.path.relpath(local_path, localdir)
					s3_path = os.path.join(destination, rel_path)
					logging.info("File {file} -> {s3}".format(file=local_path, s3=s3_path))
					b.upload_file(local_path, s3_path)
					logging.info("{local} uploaded".format(local=local_path))
				except OSError as e:
					success = False
					logging.error("Hit error while backing up: {0}".format(e))
		return success

def run_program():
	logging.info("Starting backup script...")
	logging.info("Getting settings.")
	with open("settings.json") as settings_file:
		s = json.load(settings_file)
	ba = BackupAgent(settings=s)
	if ba.perform_backup():
		logging.info("Backup successful")
	else:
		logging.info("Errors while backing up")
	logging.info("Completed")

if __name__ == '__main__':
	run_program()
