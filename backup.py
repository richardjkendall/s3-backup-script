import boto3
import logging
import json
from datetime import date

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')

class BackupAgent():

	def __init__(self, settings):
		self.s = settings
	
	def perform_backup(self):
		settings = self.s
		logging.info("Backup agent ID: {id}".format(id=settings["id"]))
		logging.info("S3 bucket ID: {id}".format(id=settings["bucket"]))
		logging.info("Number of folders to backup: {count}".format(count=len(settings["folders"])))
		d = date.today().strftime("%Y%m%d")
		logging.info("Folder in S3 bucket: {id}/{date}".format(date=d, id=settings["id"]))
		self.create_s3_session(settings["aws_profile"])
		self.create_s3_subfolder(settings["bucket"], settings["id"], d)
		for folder in settings["folders"]:
			logging.info("Working on folder: {f}".format(f=folder))
	
	def create_s3_session(self, profile):
		logging.info("Creating AWS S3 client for profile: {p}".format(p=profile))
		self.aws_session = boto3.Session(profile_name=profile, region_name="ap-southeast-2")
		self.s3client = self.aws_session.resource("s3")
	
	def create_s3_subfolder(self, bucket, agentid, d):
		b = self.s3client.Bucket(bucket)
		key = "{b}/{a}/{d}".format(b=bucket, a=agentid, d=d)
		logging.info("Checking if folder {f} exists...".format(f=key))
		objs = list(b.objects.filter(Prefix=key))
		if len(objs) > 0 and objs[0].key == key:
			logging.info("Folder exists already")
		else:
			logging.info("Folder does not exist, creating...")


def run_program():
	logging.info("Starting backup script...")
	logging.info("Getting settings.")
	with open("settings.json") as settings_file:
		s = json.load(settings_file)
	ba = BackupAgent(settings=s)
	ba.perform_backup()
	

if __name__ == '__main__':
	run_program()
