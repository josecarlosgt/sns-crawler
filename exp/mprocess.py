import os
import time
from pymongo import MongoClient

def child1():
	for i in range(0, 5):
   		print("Child1 %i in step %i" %  (os.getpid(), i))
		time.sleep(2)

def child2():
	for i in range(0, 5):
   		print("Child2 %i in step %i" %  (os.getpid(), i))
		time.sleep(2)

def parent():
	SLEEP = 10
	p = MongoClient().client["MP"].p
	p.drop(); p.insert_one({"_id": 1}); p.insert_one({"_id": 2})
	isParent = True

	newpid1 = os.fork()
	# We are the child
	if newpid1 == 0:
		isParent = False
		child1()
		p = MongoClient().client["MP"].p; p.remove({"_id": 1})
	# We are the parent
	else:
		newpid2 = os.fork()
		# We are the child
		if newpid2 == 0:
			isParent = False
			child2()
			p = MongoClient().client["MP"].p; p.remove({"_id": 2})

	if not isParent:
		print "PROCESS FINISHED"
	else:
		wait = True
		while wait:
			ps = p.find({})
			wait = False if ps.count() == 0 else True
			if wait:
				print "MAIN PROCESS WAITING: %i" % ps.count()
				time.sleep(SLEEP)

		print "MAIN PROCESS FINISHED"

parent()
