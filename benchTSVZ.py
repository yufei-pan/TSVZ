#!/usr/bin/env python3.11
import random
import TSVZ
import argparse
import time

version = '2.2'

def almost_urandom(n):
	try:
		return random.getrandbits(8 * n).to_bytes(n, 'big').decode(errors='replace')
	except OverflowError:
		return almost_urandom(n // 2) + almost_urandom(n - n // 2)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Benchmark for TSVZ")
	parser.add_argument("file_name",type=str,help="File name for the tsv file to benchmark")
	parser.add_argument("-n","--number",type=int,help="The number of entries to enter into the tsv file. Default is 1M",default=1_000_000)
	parser.add_argument('-v','--verbose',action='store_true',help="Prints out more information")
	parser.add_argument("-V","--version", action="version", version=f"%(prog)s {version}")
	args = parser.parse_args()

	fileName = args.file_name
	number = args.number
	startTime = time.perf_counter()
	testDic = TSVZ.TSVZed(fileName,rewrite_interval=0,createIfNotExist=True,rewrite_on_exit=False,rewrite_on_load=False,verbose=args.verbose,)
	#testDic = TSVZ.TSVZedLite(fileName,createIfNotExist=True)
	endTime = time.perf_counter()
	print(f"Time to create / load TSVZed object: {endTime-startTime} seconds")
	print(TSVZ.get_resource_usage())
	#print(f"Size: {testDic.__sizeof__() / 1024**2} MB")
	startTime = time.perf_counter()
	for i in range(number):
		testDic[str(i)] = [id(i)] * 20
		#sys.stdout.write(f"\r{i+1}/{number},appendQueue: {len(testDic.appendQueue)}              ")
		#sys.stdout.flush()
	#print(f"Size: {testDic.__sizeof__() / 1024**2} MB")
	testDic.close()
	del testDic
	endTime = time.perf_counter()
	print(f"Time to write {number} entries: {endTime-startTime} seconds")
	print(TSVZ.get_resource_usage())
	# testDic = TSVZ.TSVZed(fileName,rewrite_interval=0,createIfNotExist=True,rewrite_on_exit=False,rewrite_on_load=False,verbose=args.verbose)
	# testDic.memoryOnly = True
	# print(TSVZ.get_resource_usage())
	# startTime = time.perf_counter()
	# for i in range(number):
	# 	testDic[str(i)] = [str(i)]*6
	# 	#sys.stdout.write(f"\r{i+1}/{number},appendQueue: {len(testDic.appendQueue)}             ")
	# 	#sys.stdout.flush()
	# endTime = time.perf_counter()
	# print(f"Time to write {number} entries in memory only mode: {endTime-startTime} seconds")
	# print(TSVZ.get_resource_usage())
	# startTime = time.perf_counter()
	# testDic.mapToFile()
	# endTime = time.perf_counter()
	# print(f"Time to sync {number} entries: {endTime-startTime} seconds")
	# testDic.close()