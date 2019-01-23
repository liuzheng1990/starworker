from multiprocessing import cpu_count, Pool
from itertools import islice
import math
import logging
import os
from pymongo.errors import BulkWriteError
from pprint import pprint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fh = logging.FileHandler('fixing.log')
formatter = logging.Formatter('%(asctime)s, %(levelname)s: %(message)s')
fh.setFormatter(formatter)
fh.setLevel(logging.INFO)
logger.addHandler(fh)

MONGO_BATCH_SIZE = 30



class BulkWriteBase:
	"""
	Base class of the bulk write manager

	Any concrete class (dealing with a specific dbm)
	should implement `_submit_batch` method.

	This `_submit_batch` method should utilize a certain 
	bulkwrite function provided by the dbm to write the list
	of operations in `operation_batch`.

	After the child class `_submit_batch` method has done
	its job, it should explicitly call
	```
		super()._submit_batch(self)
	```
	to do the house-keeping.
	"""
	def __init__(self, batch_size=-1):
		"""
		child class __init__ method should
		do its own initialization first and then
		call this base-class __init__ function.
		"""
		if batch_size <= 0:
			self.batch_size = MONGO_BATCH_SIZE
		else:
			self.batch_size = batch_size

		self.operation_batch = []
		self.number_pending = 0
		self.number_updated = 0

	def add_operation(self, operation): # default member function
		self.operation_batch.append(operation)
		self.number_pending += 1
		if self.number_pending >= self.batch_size:
			self._submit_batch()
		return self.number_updated

	def flush_remaining(self):
		if self.number_pending > 0:
			self._submit_batch()
		return self.number_updated

	def _submit_batch(self, number_error=0):
		"""
		Any child class, when overloading this method,
		should implement its bulk_write first, before
		calling this base-class method.
		"""
		number_updated = self.number_pending - number_error
		self.number_updated += number_updated
		self.number_pending = 0
		self.operation_batch = []
		logger.info('Worker {}: updated {} docs...'.format(os.getpid(), self.number_updated))
		return self.number_updated



class BulkWriteMongo( BulkWriteBase ):
	"""
	bulk write manager for MongoDB.
	
	additional data 	: coll
	elements in `operation_batch`: UpdateOne, InsertOne ...
	bulk write method:	self.coll.bulk_write
	"""

	def __init__(self, collection, batch_size=None, ignore_bwe=False):
		self.coll = collection
		self.ignore_bwe = ignore_bwe
		super().__init__(batch_size)



	def _submit_batch(self):
		if not self.ignore_bwe:
			res = self.coll.bulk_write(self.operation_batch, ordered=False)
			return super()._submit_batch()
		else:
			try:
				res = self.coll.bulk_write(self.operation_batch, ordered=False)
			except BulkWriteError as bwe:
				number_error = len(bwe.details['writeErrors'])
				return super()._submit_batch(number_error)
			return super()._submit_batch()
		

class BulkUpsertPeewee( BulkWriteBase ):
	"""
	bulk insert manager for PeeWee databases.
	
	additional data: 					db, table_cls
	elements in `operation_batch`: 		data dictionaries
	bulk insert method:					table_cls.bulk_insert...
					(wrapped with `db.atomic()` atomic transaction.)

	"""
	def __init__(self, db, table_cls, batch_size=None):
		self.db = db
		self.table_cls = table_cls
		super().__init__(batch_size)

	def _submit_batch(self):
		with self.db.atomic():
			self.table_cls.replace_many(self.operation_batch).execute()
		return super()._submit_batch()



def chunks(count, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, count, n):
        yield (i, n)

def split_every(n, iterable):
    """
    example:

    >>> l = range(20)
    >>> it = split_every(6, l)
    >>> for i in it:
    ...     print(i)
    ...
    [0, 1, 2, 3, 4, 5]
    [6, 7, 8, 9, 10, 11]
    [12, 13, 14, 15, 16, 17]
    [18, 19]
    """
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))



class StarWorker():
	"""
	Class to execute multiple processes using `multiprocessing.Pool` Class.
	It evenly divides data array to the available CPUs.
	"""
	def __call__(self, count, target, additional_arg_list=()):
		"""
		This methods creates multiple processes and execute them in parallel.

		Number of process are 1 less then the number of available CPUs
		
		if `perform_chunks` key word arg is set to `True`, it will 
		divide the data array in equal size chunks.
		"""
		if count > 0:
			
			num_of_process = cpu_count()

			logger.debug("NUMBER OF PROCESS: {}".format(num_of_process))
			
			batch_size = int(math.ceil(count/num_of_process))
			logger.debug("Batch Size: {}".format(batch_size))
			data_chunks = chunks(count, batch_size)
			arg_lists = ((chunk,) + additional_arg_list for chunk in data_chunks)
			
			process_pool = Pool(num_of_process)
			results = process_pool.starmap(target, arg_lists)
			
			process_pool.close()
			process_pool.join()

			logger.debug("ALL PROCESSES FINISHED PROCESSING")
			
			
			return results
		
		else:
			return False

if __name__ == "__main__":
	from pymongo import MongoClient, InsertOne
	client = MongoClient()
	coll = client['increament']['test']
	l = [InsertOne({'name':'haha', 'money': 666}),
		 InsertOne({'name':'Zheng', 'money':0}),
		 InsertOne({'name':'Wuwulu', 'money':9000})]
	bwm = BulkWriteMongo(coll, 2, True)
	for op in l:
		bwm.add_operation(op)
	n_written = bwm.flush_remaining()
	print(n_written)



