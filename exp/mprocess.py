import os
import time
from pymongo import MongoClient

def child1():
	client = MongoClient()
        db = client["MP"]

	for i in range(0, 5):
   		print("Child1 %i in step %i" %  (os.getpid(), i))
		time.sleep(2)
	db.p.remove({"_id": 1})

def child2():
	client = MongoClient()
        db = client["MP"]

	for i in range(0, 5):
   		print("Child2 %i in step %i" %  (os.getpid(), i))
		time.sleep(2)
	db.p.remove({"_id": 2})

def parent():
	client = MongoClient()
        db = client["MP"]
        db.p.drop()
	db.p.insert_one({"_id": 1})
	db.p.insert_one({"_id": 2})

	isParent = True

	newpid1 = os.fork()
	
	# We are the child
	if newpid1 == 0:
		isParent = False
		child1()
	# We are the parent
	else:	
		newpid2 = os.fork()
		# We are the child
		if newpid2 == 0:
			isParent = False
			child2()
		# We are the parent
		else:
			pass

	if not isParent:
		print "A child finished"
	else:
		wait = True
		while wait:
			ps = db.p.find({})
			wait = False if ps.count() == 0 else True
			print "Parent is waiting: %i" % ps.count()
			time.sleep(1)
			
		print "Parent finished"

parent()
