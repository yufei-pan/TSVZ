#!/usr/bin/env python3
# /// script
# requires-python = ">=3.6"
# dependencies = [
# ]
# ///
import atexit
import functools
import io
import os
import re
from tabnanny import verbose
import threading
import time
import sys
from collections import OrderedDict, deque
from collections.abc import MutableMapping
RESOURCE_LIB_AVAILABLE = True
try:
	import resource
except ImportError:
	RESOURCE_LIB_AVAILABLE = False

if os.name == 'nt':
	import msvcrt
elif os.name == 'posix':
	import fcntl

version = '3.35'
__version__ = version
author = 'pan@zopyr.us'
COMMIT_DATE = '2025-11-13'

DEFAULT_DELIMITER = '\t'
DEFAULTS_INDICATOR_KEY = '#_defaults_#'

COMPRESSED_FILE_EXTENSIONS = ['gz','gzip','bz2','bzip2','xz','lzma']

def get_delimiter(delimiter,file_name = ''):
	global DEFAULT_DELIMITER
	if not delimiter:
		return DEFAULT_DELIMITER
	elif delimiter == ...:
		if not file_name:
			rtn =  '\t'
		elif file_name.endswith('.csv'):
			rtn =  ','
		elif file_name.endswith('.nsv'):
			rtn =  '\0'
		elif file_name.endswith('.psv'):
			rtn =  '|'
		else:
			rtn =  '\t'
	elif delimiter == 'comma':
		rtn =  ','
	elif delimiter == 'tab':
		rtn =  '\t'
	elif delimiter == 'pipe':
		rtn =  '|'
	elif delimiter == 'null':
		rtn =  '\0'
	else:
		rtn =  delimiter.encode().decode('unicode_escape')
	DEFAULT_DELIMITER = rtn
	return rtn

def eprint(*args, **kwargs):
	try:
		if 'file' in kwargs:
			print(*args, **kwargs)
		else:
			print(*args, file=sys.stderr, **kwargs)
	except Exception as e:
		print(f"Error: Cannot print to stderr: {e}")
		print(*args, **kwargs)

def openFileAsCompressed(fileName,mode = 'rb',encoding = 'utf8',teeLogger = None,compressLevel = 1):
	if 'b' not in mode:
		mode += 't'
	kwargs = {}
	if 'r' not in mode:
		if fileName.endswith('.xz'):
			kwargs['preset'] = compressLevel
		else:
			kwargs['compresslevel'] = compressLevel
	if 'b' not in mode:
		kwargs['encoding'] = encoding
	if fileName.endswith('.xz') or fileName.endswith('.lzma'):
		try:
			import lzma
			return lzma.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with lzma, trying bin",teeLogger=teeLogger)
	elif fileName.endswith('.gz') or fileName.endswith('.gzip'):
		try:
			import gzip
			return gzip.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with gzip, trying bin",teeLogger=teeLogger)
	elif fileName.endswith('.bz2') or fileName.endswith('.bzip2'):
		try:
			import bz2
			return bz2.open(fileName, mode, **kwargs)
		except Exception:
			__teePrintOrNot(f"Failed to open {fileName} with bz2, trying bin",teeLogger=teeLogger)
	if 't' in mode:
		mode = mode.replace('t','')
		return open(fileName, mode, encoding=encoding)
	if 'b' not in mode:
		mode += 'b'
	return open(fileName, mode)
	
def get_terminal_size():
	'''
	Get the terminal size

	@params:
		None

	@returns:
		(int,int): the number of columns and rows of the terminal
	'''
	try:
		import os
		_tsize = os.get_terminal_size()
	except Exception:
		try:
			import fcntl
			import struct
			import termios
			packed = fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack('HHHH', 0, 0, 0, 0))
			_tsize = struct.unpack('HHHH', packed)[:2]
		except Exception:
			import shutil
			_tsize = shutil.get_terminal_size(fallback=(240, 50))
	return _tsize

def pretty_format_table(data, delimiter="\t", header=None, full=False):
	version = 1.12
	_ = version
	def visible_len(s):
		return len(re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", s))
	def table_width(col_widths, sep_len):
		# total width = sum of column widths + separators between columns
		return sum(col_widths) + sep_len * (len(col_widths) - 1)
	def truncate_to_width(s, width):
		# If fits, leave as is. If too long and width >= 1, keep width-1 chars + "."
		# If width == 0, nothing fits; return empty string.
		if visible_len(s) <= width:
			return s
		if width <= 0:
			return ""
		# Build a truncated plain string based on visible chars (no ANSI awareness for slicing)
		# For simplicity, slice the raw string. This may cut ANSI; best to avoid ANSI in data if truncation occurs.
		return s[: max(width - 2, 0)] + ".."
	if not data:
		return ""
	# Normalize input data structure
	if isinstance(data, str):
		data = data.strip("\n").split("\n")
		data = [line.split(delimiter) for line in data]
	elif isinstance(data, dict):
		if isinstance(next(iter(data.values())), dict):
			tempData = [["key"] + list(next(iter(data.values())).keys())]
			tempData.extend([[key] + list(value.values()) for key, value in data.items()])
			data = tempData
		else:
			data = [[key] + list(value) for key, value in data.items()]
	elif not isinstance(data, list):
		data = list(data)
	if isinstance(data[0], dict):
		tempData = [list(data[0].keys())]
		tempData.extend([list(item.values()) for item in data])
		data = tempData
	data = [[str(item) for item in row] for row in data]
	num_cols = len(data[0])
	# Resolve header and rows
	using_provided_header = header is not None
	if not using_provided_header:
		header = data[0]
		rows = data[1:]
	else:
		if isinstance(header, str):
			header = header.split(delimiter)
		# Pad/trim header to match num_cols
		if len(header) < num_cols:
			header = header + [""] * (num_cols - len(header))
		elif len(header) > num_cols:
			header = header[:num_cols]
		rows = data
	# Compute initial column widths based on data and header
	def compute_col_widths(hdr, rows_):
		col_w = [0] * len(hdr)
		for i in range(len(hdr)):
			col_w[i] = max(0, visible_len(hdr[i]), *(visible_len(r[i]) for r in rows_ if i < len(r)))
		return col_w
	# Ensure all rows have the same number of columns
	normalized_rows = []
	for r in rows:
		if len(r) < num_cols:
			r = r + [""] * (num_cols - len(r))
		elif len(r) > num_cols:
			r = r[:num_cols]
		normalized_rows.append(r)
	rows = normalized_rows
	col_widths = compute_col_widths(header, rows)
	# If full=True, keep existing formatting
	# Else try to fit within the terminal width by:
	# 1) Switching to compressed separators if needed
	# 2) Recursively compressing columns (truncating with ".")
	sep = " | "
	hsep = "-+-"
	cols = get_terminal_size()[0]
	def render(hdr, rows, col_w, sep_str, hsep_str):
		row_fmt = sep_str.join("{{:<{}}}".format(w) for w in col_w)
		out = []
		out.append(row_fmt.format(*hdr))
		out.append(hsep_str.join("-" * w for w in col_w))
		for row in rows:
			if not any(row):
				out.append(hsep_str.join("-" * w for w in col_w))
			else:
				row = [truncate_to_width(row[i], col_w[i]) for i in range(len(row))]
				out.append(row_fmt.format(*row))
		return "\n".join(out) + "\n"
	if full:
		return render(header, rows, col_widths, sep, hsep)
	# Try default separators first
	if table_width(col_widths, len(sep)) <= cols:
		return render(header, rows, col_widths, sep, hsep)
	# Use compressed separators (no spaces)
	sep = "|"
	hsep = "+"
	if table_width(col_widths, len(sep)) <= cols:
		return render(header, rows, col_widths, sep, hsep)
	# Begin column compression
	# Track which columns have been compressed already to header width
	header_widths = [visible_len(h) for h in header]
	width_diff = [max(col_widths[i] - header_widths[i],0) for i in range(num_cols)]
	total_overflow_width = table_width(col_widths, len(sep)) - cols
	for i, diff in sorted(enumerate(width_diff), key=lambda x: -x[1]):
		if total_overflow_width <= 0:
			break
		if diff <= 0:
			continue
		reduce_by = min(diff, total_overflow_width)
		col_widths[i] -= reduce_by
		total_overflow_width -= reduce_by
	return render(header, rows, col_widths, sep, hsep)

def format_bytes(size, use_1024_bytes=None, to_int=False, to_str=False,str_format='.2f'):
	"""
	Format the size in bytes to a human-readable format or vice versa.
	From hpcp: https://github.com/yufei-pan/hpcp

	Args:
		size (int or str): The size in bytes or a string representation of the size.
		use_1024_bytes (bool, optional): Whether to use 1024 bytes as the base for conversion. If None, it will be determined automatically. Default is None.
		to_int (bool, optional): Whether to convert the size to an integer. Default is False.
		to_str (bool, optional): Whether to convert the size to a string representation. Default is False.
		str_format (str, optional): The format string to use when converting the size to a string. Default is '.2f'.

	Returns:
		int or str: The formatted size based on the provided arguments.

	Examples:
		>>> format_bytes(1500, use_1024_bytes=False)
		'1.50 K'
		>>> format_bytes('1.5 GiB', to_int=True)
		1610612736
		>>> format_bytes('1.5 GiB', to_str=True)
		'1.50 Gi'
		>>> format_bytes(1610612736, use_1024_bytes=True, to_str=True)
		'1.50 Gi'
		>>> format_bytes(1610612736, use_1024_bytes=False, to_str=True)
		'1.61 G'
	"""
	if to_int or isinstance(size, str):
		if isinstance(size, int):
			return size
		elif isinstance(size, str):
			# Use regular expression to split the numeric part from the unit, handling optional whitespace
			match = re.match(r"(\d+(\.\d+)?)\s*([a-zA-Z]*)", size)
			if not match:
				if to_str:
					return size
				print("Invalid size format. Expected format: 'number [unit]', e.g., '1.5 GiB' or '1.5GiB'")
				print(f"Got: {size}")
				return 0
			number, _, unit = match.groups()
			number = float(number)
			unit  = unit.strip().lower().rstrip('b')
			# Define the unit conversion dictionary
			if unit.endswith('i'):
				# this means we treat the unit as 1024 bytes if it ends with 'i'
				use_1024_bytes = True
			elif use_1024_bytes is None:
				use_1024_bytes = False
			unit  = unit.rstrip('i')
			if use_1024_bytes:
				power = 2**10
			else:
				power = 10**3
			unit_labels = {'': 0, 'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5}
			if unit not in unit_labels:
				if to_str:
					return size
				print(f"Invalid unit '{unit}'. Expected one of {list(unit_labels.keys())}")
				return 0
			if to_str:
				return format_bytes(size=int(number * (power ** unit_labels[unit])), use_1024_bytes=use_1024_bytes, to_str=True, str_format=str_format)
			# Calculate the bytes
			return int(number * (power ** unit_labels[unit]))
		else:
			try:
				return int(size)
			except Exception:
				return 0
	elif to_str or isinstance(size, int) or isinstance(size, float):
		if isinstance(size, str):
			try:
				size = size.rstrip('B').rstrip('b')
				size = float(size.lower().strip())
			except Exception:
				return size
		# size is in bytes
		if use_1024_bytes or use_1024_bytes is None:
			power = 2**10
			n = 0
			power_labels = {0 : '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti', 5: 'Pi'}
			while size > power:
				size /= power
				n += 1
			return f"{size:{str_format}}{' '}{power_labels[n]}"
		else:
			power = 10**3
			n = 0
			power_labels = {0 : '', 1: 'K', 2: 'M', 3: 'G', 4: 'T', 5: 'P'}
			while size > power:
				size /= power
				n += 1
			return f"{size:{str_format}}{' '}{power_labels[n]}"
	else:
		try:
			return format_bytes(float(size), use_1024_bytes)
		except Exception as e:
			import traceback
			print(f"Error: {e}")
			print(traceback.format_exc())
			print(f"Invalid size: {size}")
		return 0

def get_resource_usage(return_dict = False):
	try:
		if RESOURCE_LIB_AVAILABLE:
			rawResource =  resource.getrusage(resource.RUSAGE_SELF)
			resourceDict = {}
			resourceDict['user mode time'] = f'{rawResource.ru_utime} seconds'
			resourceDict['system mode time'] = f'{rawResource.ru_stime} seconds'
			resourceDict['max resident set size'] = f'{format_bytes(rawResource.ru_maxrss * 1024)}B'
			resourceDict['shared memory size'] = f'{format_bytes(rawResource.ru_ixrss * 1024)}B'
			resourceDict['unshared memory size'] = f'{format_bytes(rawResource.ru_idrss * 1024)}B'
			resourceDict['unshared stack size'] = f'{format_bytes(rawResource.ru_isrss * 1024)}B'
			resourceDict['cached page hits'] = f'{rawResource.ru_minflt}'
			resourceDict['missed page hits'] = f'{rawResource.ru_majflt}'
			resourceDict['swapped out page count'] = f'{rawResource.ru_nswap}'
			resourceDict['block input operations'] = f'{rawResource.ru_inblock}'
			resourceDict['block output operations'] = f'{rawResource.ru_oublock}'
			resourceDict['IPC messages sent'] = f'{rawResource.ru_msgsnd}'
			resourceDict['IPC messages received'] = f'{rawResource.ru_msgrcv}'
			resourceDict['signals received'] = f'{rawResource.ru_nsignals}'
			resourceDict['voluntary context sw'] = f'{rawResource.ru_nvcsw}'
			resourceDict['involuntary context sw'] = f'{rawResource.ru_nivcsw}'
			if return_dict:
				return resourceDict
			return '\n'.join(['\t'.join(line) for line in resourceDict.items()])
	except Exception as e:
		print(f"Error: {e}")
	if return_dict:
		return {}
	return ''

def __teePrintOrNot(message,level = 'info',teeLogger = None):
	"""
	Prints the given message or logs it using the provided teeLogger.

	Parameters:
		message (str): The message to be printed or logged.
		level (str, optional): The log level. Defaults to 'info'.
		teeLogger (object, optional): The logger object used for logging. Defaults to None.

	Returns:
		None
	"""
	try:
		if teeLogger:
			try:
				teeLogger.teelog(message,level,callerStackDepth=3)
			except Exception:
				teeLogger.teelog(message,level)
		else:
			print(message,flush=True)
	except Exception:
		print(message,flush=True)

def _processLine(line,taskDic,correctColumnNum,strict = True,delimiter = DEFAULT_DELIMITER,defaults = ...,
				 storeOffset = False, offset = -1):
	"""
	Process a line of text and update the task dictionary.

	Parameters:
	line (str): The line of text to process.
	taskDic (dict): The dictionary to update with the processed line.
	correctColumnNum (int): The expected number of columns in the line.
	strict (bool, optional): Whether to strictly enforce the correct number of columns. Defaults to True.
	defaults (list, optional): The default values to use for missing columns. Defaults to [].
	storeOffset (bool, optional): Whether to store the offset of the line in the taskDic. Defaults to False.
	offset (int, optional): The offset of the line in the file. Defaults to -1.

	Returns:
	tuple: A tuple containing the updated correctColumnNum and the processed lineCache or offset.

	"""
	if defaults is ...:
		defaults = []
	line = line.strip('\x00').rstrip('\r\n')
	if not line or (line.startswith('#') and not line.startswith(DEFAULTS_INDICATOR_KEY)):
		# if verbose:
		# 	__teePrintOrNot(f"Ignoring comment line: {line}",teeLogger=teeLogger)
		return correctColumnNum , []
	# we only interested in the lines that have the correct number of columns
	lineCache = _unsanitize(line.split(delimiter),delimiter)
	if not lineCache or not lineCache[0]:
		return correctColumnNum , []
	if correctColumnNum == -1:
		if defaults and len(defaults) > 1:
			correctColumnNum = len(defaults)
		else:
			correctColumnNum = len(lineCache)
		# if verbose:
		# 	__teePrintOrNot(f"detected correctColumnNum: {len(lineCache)}",teeLogger=teeLogger)
	if len(lineCache) == 1 or not any(lineCache[1:]):
		if correctColumnNum == 1: 
			taskDic[lineCache[0]] = lineCache if not storeOffset else offset
		elif lineCache[0] == DEFAULTS_INDICATOR_KEY:
			# if verbose:
			# 	__teePrintOrNot(f"Empty defaults line found: {line}",teeLogger=teeLogger)
			defaults.clear()
			defaults[0] = DEFAULTS_INDICATOR_KEY
		else:
			# if verbose:
			# 	__teePrintOrNot(f"Key {lineCache[0]} found with empty value, deleting such key's representaion",teeLogger=teeLogger)
			if lineCache[0] in taskDic:
				del taskDic[lineCache[0]]
		return correctColumnNum , []
	elif len(lineCache) != correctColumnNum:
		if strict and not any(defaults[1:]):
			# if verbose:
			# 	__teePrintOrNot(f"Ignoring line with {len(lineCache)} columns: {line}",teeLogger=teeLogger)
			return correctColumnNum , []
		else:
			# fill / cut the line with empty entries til the correct number of columns
			if len(lineCache) < correctColumnNum:
				lineCache += ['']*(correctColumnNum-len(lineCache))
			elif len(lineCache) > correctColumnNum:
				lineCache = lineCache[:correctColumnNum]
			# if verbose:
			# 	__teePrintOrNot(f"Correcting {lineCache[0]}",teeLogger=teeLogger)
	# now replace empty values with defaults
	if defaults and len(defaults) > 1:
		for i in range(1,len(lineCache)):
			if not lineCache[i] and i < len(defaults) and defaults[i]:
				lineCache[i] = defaults[i]
	if lineCache[0] == DEFAULTS_INDICATOR_KEY:
		# if verbose:
		# 	__teePrintOrNot(f"Defaults line found: {line}",teeLogger=teeLogger)
		defaults[:] = lineCache
		return correctColumnNum , []
	taskDic[lineCache[0]] = lineCache if not storeOffset else offset
	# if verbose:
	# 	__teePrintOrNot(f"Key {lineCache[0]} added",teeLogger=teeLogger)
	return correctColumnNum, lineCache

def read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None, strict=False,
						 encoding = 'utf8',delimiter = ...,defaults = ...,storeOffset = False	):
	"""
	Reads the last valid line from a file.

	Args:
		fileName (str): The name of the file to read.
		taskDic (dict): A dictionary to pass to processLine function.
		correctColumnNum (int): A column number to pass to processLine function.
		verbose (bool, optional): Whether to print verbose output. Defaults to False.
		teeLogger (optional): Logger to use for tee print. Defaults to None.
		encoding (str, optional): The encoding of the file. Defaults to None.
		strict (bool, optional): Whether to enforce strict processing. Defaults to False.
		delimiter (str, optional): The delimiter used in the file. Defaults to None.
		defaults (list, optional): The default values to use for missing columns. Defaults to [].
		storeOffset (bool, optional): Instead of storing the data in taskDic, store the offset of each line. Defaults to False.

	Returns:
		list: The last valid line as a list of strings, or an empty list if no valid line is found.
	"""
	chunk_size = 1024  # Read in chunks of 1024 bytes
	last_valid_line = []
	if defaults is ...:
		defaults = []
	delimiter = get_delimiter(delimiter,file_name=fileName)
	if verbose:
		__teePrintOrNot(f"Reading last line only from {fileName}",teeLogger=teeLogger)
	with openFileAsCompressed(fileName, 'rb',encoding=encoding, teeLogger=teeLogger) as file:
		file.seek(0, os.SEEK_END)
		file_size = file.tell()
		buffer = b''
		position = file_size
		processedSize = 0

		while position > 0:
			# Read chunks from the end of the file
			read_size = min(chunk_size, position)
			position -= read_size
			file.seek(position)
			chunk = file.read(read_size)
			
			# Prepend new chunk to buffer
			buffer = chunk + buffer
			
			# Split the buffer into lines
			lines = buffer.split(b'\n')
			
			# Process lines from the last to the first
			for i in range(len(lines) - 1, -1, -1):
				processedSize += len(lines[i]) + 1  # +1 for the newline character
				if lines[i].strip():  # Skip empty lines
					# Process the line
					correctColumnNum, lineCache = _processLine(
						line=lines[i].decode(encoding=encoding,errors='replace'),
						taskDic=taskDic,
						correctColumnNum=correctColumnNum,
						strict=strict,
						delimiter=delimiter,
						defaults=defaults,
						storeOffset=storeOffset,
						offset=file_size - processedSize + 1
					)
					# If the line is valid, return it
					if lineCache:
						if storeOffset and any(lineCache):
							return lineCache
			
			# Keep the last (possibly incomplete) line in buffer for the next read
			buffer = lines[0]

	# Return empty list if no valid line found
	if storeOffset:
		return -1
	return last_valid_line

@functools.lru_cache(maxsize=None)
def _get_sanitization_re(delimiter = DEFAULT_DELIMITER):
	return re.compile(r"(</sep/>|</LF/>|<sep>|<LF>|\n|" + re.escape(delimiter) + r")")

_sanitize_replacements = {
	"<sep>":"</sep/>",
	"<LF>":"</LF/>",
	"\n":"<LF>",
}
_inverse_sanitize_replacements = {v: k for k, v in _sanitize_replacements.items()}

def _sanitize(data,delimiter = DEFAULT_DELIMITER):
	if not data:
		return data
	def repl(m):
		tok = m.group(0)
		if tok == delimiter:
			return "<sep>"
		if tok in ("</sep/>", "</LF/>"):
			eprint(f"Warning: Found illegal token '{tok}' during sanitization. It will be replaced.")
		return _sanitize_replacements.get(tok, tok)
	pattern = _get_sanitization_re(delimiter)
	if isinstance(data,str):
		return pattern.sub(repl, data)
	else:
		return [pattern.sub(repl,str(segment)) if segment else '' for segment in data]

def _unsanitize(data,delimiter = DEFAULT_DELIMITER):
	if not data:
		return data
	def repl(m):
		tok = m.group(0)
		if tok == "<sep>":
			return delimiter
		return _inverse_sanitize_replacements.get(tok, tok)
	pattern = _get_sanitization_re(delimiter)
	if isinstance(data,str):
		return pattern.sub(repl, data.rstrip())
	else:
		return [pattern.sub(repl,str(segment).rstrip()) if segment else '' for segment in data]

def _formatHeader(header,verbose = False,teeLogger = None,delimiter = DEFAULT_DELIMITER):
	"""
	Format the header string.

	Parameters:
	- header (str or list): The header string or list to format.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- teeLogger (object, optional): The tee logger object for printing output. Defaults to None.

	Returns:
		list: The formatted header list of string.
	"""
	if isinstance(header,str):
		header = header.split(delimiter)
	else:
		try:
			header = [str(s) for s in header]
		except Exception:
			if verbose:
				__teePrintOrNot('Invalid header, setting header to empty.','error',teeLogger=teeLogger)
			header = []
	return [s.rstrip() for s in header]

def _lineContainHeader(header,line,verbose = False,teeLogger = None,strict = False,delimiter = DEFAULT_DELIMITER):
	"""
	Verify if a line contains the header.

	Parameters:
	- header (str): The header string to verify.
	- line (str): The line to verify against the header.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to False.

	Returns:
	bool: True if the header matches the line, False otherwise.
	"""
	line = _formatHeader(line,verbose=verbose,teeLogger=teeLogger,delimiter=delimiter)
	if verbose:
		__teePrintOrNot(f"Header: \n{header}",teeLogger=teeLogger)
		__teePrintOrNot(f"First line: \n{line}",teeLogger=teeLogger)
	if len(header) != len(line) or any([header[i] not in line[i] for i in range(len(header))]):
		__teePrintOrNot(f"Header mismatch: \n{line} \n!= \n{header}",teeLogger=teeLogger)
		if strict:
			raise ValueError("Data format error! Header mismatch")
		return False
	return True

def _verifyFileExistence(fileName,createIfNotExist = True,teeLogger = None,header = [],encoding = 'utf8',strict = True,delimiter = DEFAULT_DELIMITER):
	"""
	Verify the existence of the tabular file.

	Parameters:
	- fileName (str): The path of the tabular file.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to True.
	- teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
	- header (list, optional): The header line to verify against. Defaults to [].
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.

	Returns:
	bool: True if the file exists, False otherwise.
	"""
	remainingFileName, _ ,extenstionName = fileName.rpartition('.')
	if extenstionName in COMPRESSED_FILE_EXTENSIONS:
		remainingFileName, _ ,extenstionName = remainingFileName.rpartition('.')
	if delimiter and delimiter == '\t' and not extenstionName == 'tsv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .tsv','warning',teeLogger=teeLogger)
	elif delimiter and delimiter == ',' and not extenstionName == 'csv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .csv','warning',teeLogger=teeLogger)
	elif delimiter and delimiter == '\0' and not extenstionName == 'nsv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .nsv','warning',teeLogger=teeLogger)
	elif delimiter and delimiter == '|' and not extenstionName == 'psv':
		__teePrintOrNot(f'Warning: Filename {fileName} does not end with .psv','warning',teeLogger=teeLogger)
	if not os.path.isfile(fileName):
		if createIfNotExist:
			try:
				with openFileAsCompressed(fileName, mode ='wb',encoding=encoding,teeLogger=teeLogger)as file:
					header = delimiter.join(_sanitize(_formatHeader(header,
													 verbose=verbose,
													 teeLogger=teeLogger,
													 delimiter=delimiter,
													 ),delimiter=delimiter))
					file.write(header.encode(encoding=encoding,errors='replace')+b'\n')
				__teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
				return True
			except Exception:
				__teePrintOrNot('Failed to create '+fileName,'error',teeLogger=teeLogger)
				if strict:
					raise FileNotFoundError("Failed to create file")
				return False
		elif strict:
			__teePrintOrNot('File not found','error',teeLogger=teeLogger)
			raise FileNotFoundError("File not found")
		else:
			return False
	return True

def readTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,
			verbose = False,taskDic = None,encoding = 'utf8',strict = True,delimiter = '\t',defaults = ...,
			correctColumnNum = -1):
	"""
	Compatibility method, calls readTabularFile. 
	Read a Tabular (CSV / TSV / NSV) file and return the data as a dictionary.

	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t'.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].
	- correctColumnNum (int, optional): The expected number of columns in the file. If -1, it will be determined from the first valid line. Defaults to -1.

	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	return readTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,
						   lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,
						   encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults,
						   correctColumnNum = correctColumnNum)

def readTabularFile(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,
					verbose = False,taskDic = None,encoding = 'utf8',strict = True,delimiter = ...,defaults = ...,
					correctColumnNum = -1,storeOffset = False):
	"""
	Read a Tabular (CSV / TSV / NSV) file and return the data as a dictionary.

	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].
	- correctColumnNum (int, optional): The expected number of columns in the file. If -1, it will be determined from the first valid line. Defaults to -1.
	- storeOffset (bool, optional): Instead of storing the data in taskDic, store the offset of each line. Defaults to False.
	
	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	if taskDic is None:
		taskDic = {}
	if defaults is ...:
		defaults = []
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger, delimiter = delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
		return taskDic
	with openFileAsCompressed(fileName, mode ='rb',encoding=encoding,teeLogger=teeLogger)as file:
		if any(header) and verifyHeader:
				line = file.readline().decode(encoding=encoding,errors='replace')
				if _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict) and correctColumnNum == -1:
					correctColumnNum = len(header)
					if verbose:
						__teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
		if lastLineOnly:
			lineCache = read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter, defaults=defaults,storeOffset=storeOffset)
			# if lineCache:
			# 	taskDic[lineCache[0]] = lineCache
			return lineCache
		for line in file:
			correctColumnNum, _ = _processLine(line.decode(encoding=encoding,errors='replace'),taskDic,correctColumnNum,strict = strict,delimiter=delimiter,defaults = defaults,storeOffset=storeOffset,offset=file.tell()-len(line))
	return taskDic

def appendTSV(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = '\t'):
	"""
	Compatibility method, calls appendTabularFile.
	Append a line of data to a Tabular file.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- lineToAppend (str or list): The line of data to append. If it is a string, it will be split by delimiter to form a list.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	Raises:
	- Exception: If the file does not exist and createIfNotExist is False.
	- Exception: If the existing header does not match the provided header.
	"""
	return appendTabularFile(fileName,lineToAppend,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding, strict = strict, delimiter = delimiter)

def appendTabularFile(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = ...):
	"""
	Append a line of data to a Tabular file.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- lineToAppend (str or list): The line of data to append. If it is a string, it will be split by delimiter to form a list.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	Raises:
	- Exception: If the file does not exist and createIfNotExist is False.
	- Exception: If the existing header does not match the provided header.
	"""
	return appendLinesTabularFile(fileName,[lineToAppend],teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding, strict = strict, delimiter = delimiter)

def appendLinesTabularFile(fileName,linesToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = ...):
	"""
	Append lines of data to a Tabular file.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- linesToAppend (list): The lines of data to append. If it is a list of string, then each string will be split by delimiter to form a list.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	Raises:
	- Exception: If the file does not exist and createIfNotExist is False.
	- Exception: If the existing header does not match the provided header.
	"""
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
		return
	formatedLines = []
	for line in linesToAppend:
		if isinstance(linesToAppend,dict):
			key = line
			line = linesToAppend[key]
		if isinstance(line,str):
			line = line.split(delimiter)
		elif line:
			for i in range(len(line)):
				if not isinstance(line[i],str):
					try:
						line[i] = str(line[i]).rstrip()
					except Exception as e:
						line[i] = str(e)
		if isinstance(linesToAppend,dict):
			if (not line or line[0] != key):
				line = [key]+line
		formatedLines.append(_sanitize(line,delimiter=delimiter))
	if not formatedLines:
		if verbose:
			__teePrintOrNot(f"No lines to append to {fileName}",teeLogger=teeLogger)
		return
	correctColumnNum = max([len(line) for line in formatedLines])
	if any(header) and verifyHeader:
		with openFileAsCompressed(fileName, mode ='rb',encoding=encoding,teeLogger=teeLogger)as file:
			line = file.readline().decode(encoding=encoding,errors='replace')
			if _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
				correctColumnNum = len(header)
				if verbose:
					__teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
	# truncate / fill the lines to the correct number of columns
	for i in range(len(formatedLines)):
		if len(formatedLines[i]) < correctColumnNum:
			formatedLines[i] += ['']*(correctColumnNum-len(formatedLines[i]))
		elif len(formatedLines[i]) > correctColumnNum:
			formatedLines[i] = formatedLines[i][:correctColumnNum]
	with openFileAsCompressed(fileName, mode ='ab',encoding=encoding,teeLogger=teeLogger)as file:
		# check if the file ends in a newline
		# file.seek(-1, os.SEEK_END)
		# if file.read(1) != b'\n':
		#     file.write(b'\n')
		file.write(b'\n'.join([delimiter.join(line).encode(encoding=encoding,errors='replace') for line in formatedLines]) + b'\n')
		if verbose:
			__teePrintOrNot(f"Appended {len(formatedLines)} lines to {fileName}",teeLogger=teeLogger)

def clearTSV(fileName,teeLogger = None,header = '',verifyHeader = False,verbose = False,encoding = 'utf8',strict = False,delimiter = '\t'):
	"""
	Compatibility method, calls clearTabularFile.
	Clear the contents of a Tabular file. Will create if not exist.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	"""
	return clearTabularFile(fileName,teeLogger = teeLogger,header = header,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding,strict = strict,delimiter = delimiter)

def clearTabularFile(fileName,teeLogger = None,header = '',verifyHeader = False,verbose = False,encoding = 'utf8',strict = False,delimiter = ...):
	"""
	Clear the contents of a Tabular file. Will create if not exist.
	Parameters:
	- fileName (str): The path of the Tabular file.
	- teeLogger (optional): A logger object for logging messages.
	- header (str or list, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
	- verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
	- verbose (bool, optional): If True, additional information will be printed during the execution.
	- encoding (str, optional): The encoding of the file.
	- strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
	"""
	delimiter = get_delimiter(delimiter,file_name=fileName)
	header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
	if not _verifyFileExistence(fileName,createIfNotExist = True,teeLogger = teeLogger,header = header,encoding = encoding,strict = False,delimiter=delimiter):
		raise FileNotFoundError("Something catastrophic happened! File still not found after creation")
	else:
		with openFileAsCompressed(fileName, mode ='rb',encoding=encoding,teeLogger=teeLogger)as file:
			if any(header) and verifyHeader:
				line = file.readline().decode(encoding=encoding,errors='replace')
				if not _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
					__teePrintOrNot(f'Warning: Header mismatch in {fileName}. Keeping original header in file...','warning',teeLogger)
				header = _formatHeader(line,verbose = verbose,teeLogger = teeLogger,delimiter=delimiter)
		with openFileAsCompressed(fileName, mode ='wb',encoding=encoding,teeLogger=teeLogger)as file:
			if header:
				header = delimiter.join(_sanitize(header,delimiter=delimiter))
				file.write(header.encode(encoding=encoding,errors='replace')+b'\n')
	if verbose:
		__teePrintOrNot(f"Cleared {fileName}",teeLogger=teeLogger)

def getFileUpdateTimeNs(fileName):
	# return 0 if the file does not exist
	if not os.path.isfile(fileName):
		return 0
	try:
		return os.stat(fileName).st_mtime_ns
	except Exception:
		__teePrintOrNot(f"Failed to get file update time for {fileName}",'error')
		return get_time_ns()

def get_time_ns():
	try:
		return time.time_ns()
	except Exception:
		# try to get the time in nanoseconds
		return int(time.time()*1e9)

def scrubTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False,taskDic = None,encoding = 'utf8',strict = False,delimiter = '\t',defaults = ...):
	"""
	Compatibility method, calls scrubTabularFile.
	Scrub a Tabular (CSV / TSV / NSV) file by reading it and writing the contents back into the file.
	Return the data as a dictionary.

	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to False.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].

	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	return scrubTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults)

def scrubTabularFile(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,
					 verbose = False,taskDic = None,encoding = 'utf8',strict = False,delimiter = ...,defaults = ...,correctColumnNum = -1):
	"""
	Scrub a Tabular (CSV / TSV / NSV) file by reading it and writing the contents back into the file.
	If using compressed files. This will recompress the file in whole and possibily increase the compression ratio reducing the file size.
	Return the data as a dictionary.
	
	Parameters:
	- fileName (str): The path to the Tabular file.
	- teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
	- header (str or list, optional): The header of the Tabular file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
	- createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
	- lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
	- verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
	- verbose (bool, optional): Whether to print verbose output. Defaults to False.
	- taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
	- encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
	- strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to False.
	- delimiter (str, optional): The delimiter used in the Tabular file. Defaults to '\t' for TSV, ',' for CSV, '\0' for NSV.
	- defaults (list, optional): The default values to use for missing columns. Defaults to [].
	- correctColumnNum (int, optional): The expected number of columns in the file. If -1, it will be determined from the first valid line. Defaults to -1.

	Returns:
	- OrderedDict: The dictionary containing the data from the Tabular file.

	Raises:
	- Exception: If the file is not found or there is a data format error.

	"""
	file =  readTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,
							lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,
							encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults,correctColumnNum = correctColumnNum)
	if file:
		clearTabularFile(fileName,teeLogger = teeLogger,header = header,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding,strict = strict,delimiter = delimiter)
		appendLinesTabularFile(fileName,file,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,verifyHeader = verifyHeader,verbose = verbose,encoding = encoding,strict = strict,delimiter = delimiter)
	return file

def getListView(tsvzDic,header = [],delimiter = DEFAULT_DELIMITER):
	if header:
		if isinstance(header,str):
			header = header.split(delimiter)
		elif not isinstance(header,list):
			try:
				header = list(header)
			except Exception:
				header = []
	if not tsvzDic:
		if not header:
			return []
		else:
			return [header]
	if not header:
		return list(tsvzDic.values())
	else:
		values = list(tsvzDic.values())
		if values[0] and values[0] == header:
			return values
		else:
			return [header] + values

# create a tsv class that functions like a ordered dictionary but will update the file when modified
class TSVZed(OrderedDict):
	"""
	A thread-safe, file-backed ordered dictionary for managing TSV (Tab-Separated Values) files.
	TSVZed extends OrderedDict to provide automatic synchronization between an in-memory
	dictionary and a TSV file on disk. It supports concurrent file access, automatic
	persistence, and configurable sync strategies.
	Parameters
	----------
	fileName : str
		Path to the TSV file to be managed.
	teeLogger : object, optional
		Logger object with a teelog method for logging messages. If None, uses print.
	header : str, optional
		Column header line for the TSV file. Used for validation and file creation.
	createIfNotExist : bool, default=True
		If True, creates the file if it doesn't exist.
	verifyHeader : bool, default=True
		If True, verifies that the file header matches the provided header.
	rewrite_on_load : bool, default=True
		If True, rewrites the entire file when loading to ensure consistency.
	rewrite_on_exit : bool, default=False
		If True, rewrites the entire file when closing/exiting.
	rewrite_interval : float, default=0
		Minimum time interval (in seconds) between full file rewrites. 0 means no limit.
	append_check_delay : float, default=0.01
		Time delay (in seconds) between checks of the append queue by the worker thread.
	monitor_external_changes : bool, default=True
		If True, monitors and detects external file modifications.
	verbose : bool, default=False
		If True, prints detailed operation logs.
	encoding : str, default='utf8'
		Character encoding for reading/writing the file.
	delimiter : str, optional
		Field delimiter character. Auto-detected from filename if not specified.
	defaults : list or str, optional
		Default values for columns when values are missing.
	strict : bool, default=False
		If True, enforces strict validation of column counts and raises errors on mismatch.
	correctColumnNum : int, default=-1
		Expected number of columns. -1 means auto-detect from header or first record.
	Attributes
	----------
	version : str
		Version of the TSVZed implementation.
	dirty : bool
		True if the in-memory data differs from the file on disk.
	deSynced : bool
		True if synchronization with the file has failed or external changes detected.
	memoryOnly : bool
		If True, changes are kept in memory only and not written to disk.
	appendQueue : deque
		Queue of lines waiting to be appended to the file.
	writeLock : threading.Lock
		Lock for ensuring thread-safe file operations.
	shutdownEvent : threading.Event
		Event signal for stopping the append worker thread.
	appendThread : threading.Thread
		Background thread that handles asynchronous file appending.
	Methods
	-------
	load()
		Load or reload data from the TSV file.
	reload()
		Refresh data from the TSV file, discarding in-memory changes.
	rewrite(force=False, reloadInternalFromFile=None)
		Rewrite the entire file with current in-memory data.
	mapToFile()
		Synchronize in-memory data to the file using in-place updates.
	hardMapToFile()
		Completely rewrite the file from scratch with current data.
	clear()
		Clear all data from memory and optionally the file.
	clear_file()
		Clear the file, keeping only the header.
	commitAppendToFile()
		Write all queued append operations to the file.
	stopAppendThread()
		Stop the background append worker thread and perform final sync.
	setDefaults(defaults)
		Set default values for columns.
	getListView()
		Get a list representation of the data with headers.
	getResourceUsage(return_dict=False)
		Get current resource usage statistics.
	checkExternalChanges()
		Check if the file has been modified externally.
	close()
		Close the TSVZed object, stopping background threads and syncing data.
	Notes
	-----
	- The class uses a background thread to handle asynchronous file operations.
	- File locking is implemented for both POSIX and Windows systems.
	- Keys starting with '#' are treated as comments and not persisted to file.
	- The special key '#DEFAULTS#' is used to store column default values.
	- Supports compressed file formats through automatic detection.
	- Thread-safe for concurrent access from multiple threads.
	Examples
	--------
	>>> with TSVZed('data.tsv', header='id\tname\tvalue') as tsv:
	...     tsv['key1'] = ['key1', 'John', '100']
	...     tsv['key2'] = ['key2', 'Jane', '200']
	...     print(tsv['key1'])
	['key1', 'John', '100']
	>>> tsv = TSVZed('data.tsv', verbose=True, rewrite_on_exit=True)
	>>> tsv['key3'] = 'key3\tBob\t300'
	>>> tsv.close()
	"""
	def __teePrintOrNot(self,message,level = 'info'):
		try:
			if self.teeLogger:
				self.teeLogger.teelog(message,level)
			else:
				print(message,flush=True)
		except Exception:
			print(message,flush=True)

	def getResourceUsage(self,return_dict = False):
		return get_resource_usage(return_dict = return_dict)

	def __init__ (self,fileName,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,
				  rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,monitor_external_changes = True,
				  verbose = False,encoding = 'utf8',delimiter = ...,defaults = None,strict = False,correctColumnNum = -1):
		super().__init__()
		self.version = version
		self.strict = strict
		self.externalFileUpdateTime = getFileUpdateTimeNs(fileName)
		self.lastUpdateTime = self.externalFileUpdateTime
		self._fileName = fileName
		self.teeLogger = teeLogger
		self.delimiter = get_delimiter(delimiter,file_name=fileName)
		self.setDefaults(defaults)
		self.header = _formatHeader(header,verbose = verbose,teeLogger = self.teeLogger,delimiter=self.delimiter)
		self.correctColumnNum = correctColumnNum
		self.createIfNotExist = createIfNotExist
		self.verifyHeader = verifyHeader
		self.rewrite_on_load = rewrite_on_load
		self.rewrite_on_exit = rewrite_on_exit
		self.rewrite_interval = rewrite_interval
		self.monitor_external_changes = monitor_external_changes
		if not monitor_external_changes:
			self.__teePrintOrNot(f"Warning: External changes monitoring disabled for {self._fileName}. Will overwrite external changes.",'warning')
		self.verbose = verbose
		if append_check_delay < 0:
			append_check_delay = 0.00001
			self.__teePrintOrNot('append_check_delay cannot be less than 0, setting it to 0.00001','error')
		self.append_check_delay = append_check_delay
		self.appendQueue = deque()
		self.dirty = False
		self.deSynced = False
		self.memoryOnly = False
		self.encoding = encoding
		self.writeLock = threading.Lock()
		self.shutdownEvent = threading.Event()
		#self.appendEvent = threading.Event()
		self.appendThread  = threading.Thread(target=self._appendWorker,daemon=True)
		self.appendThread.start()
		self.load()
		atexit.register(self.stopAppendThread)

	def setDefaults(self,defaults):
		if not defaults:
			defaults = []
		if isinstance(defaults,str):
			defaults = defaults.split(self.delimiter)
		elif not isinstance(defaults,list):
			try:
				defaults = list(defaults)
			except Exception:
				if self.verbose:
					self.__teePrintOrNot('Invalid defaults, setting defaults to empty.','error')
				defaults = []
		defaults = [str(s).rstrip() if s else '' for s in defaults]
		if not any(defaults):
			defaults = []
		if not defaults or defaults[0] != DEFAULTS_INDICATOR_KEY:
			defaults = [DEFAULTS_INDICATOR_KEY]+defaults
		self.defaults = defaults

	def load(self):
		self.reload()
		if self.rewrite_on_load:
			self.rewrite(force = True,reloadInternalFromFile = False)
		return self

	def reload(self):
		# Load or refresh data from the TSV file
		mo = self.memoryOnly
		self.memoryOnly = True
		if self.verbose:
			self.__teePrintOrNot(f"Loading {self._fileName}")
		super().clear()
		readTabularFile(self._fileName, teeLogger = self.teeLogger, header = self.header,
					createIfNotExist = self.createIfNotExist, verifyHeader = self.verifyHeader,
					verbose = self.verbose, taskDic = self,encoding = self.encoding if self.encoding else None,
					strict = self.strict, delimiter = self.delimiter, defaults=self.defaults)
		if self.verbose:
			self.__teePrintOrNot(f"Loaded {len(self)} records from {self._fileName}")
		if self.header and any(self.header) and self.verifyHeader:
			self.correctColumnNum = len(self.header)
		elif self:
			self.correctColumnNum = len(self[next(iter(self))])
		else:
			self.correctColumnNum = -1
		if self.verbose:
			self.__teePrintOrNot(f"correctColumnNum: {self.correctColumnNum}")
		#super().update(loadedData)
		if self.verbose:
			self.__teePrintOrNot(f"TSVZed({self._fileName}) loaded")
		self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)
		self.lastUpdateTime = self.externalFileUpdateTime
		self.memoryOnly = mo
		return self

	def __setitem__(self,key,value):
		key = str(key).rstrip()
		if not key:
			self.__teePrintOrNot('Key cannot be empty','error')
			return
		if isinstance(value,str):
			value = value.split(self.delimiter)
		# sanitize the value
		value = [str(s).rstrip() if s else '' for s in value]
		# the first field in value should be the key
		# add it if it is not there
		if not value or value[0] != key:
			value = [key]+value
		# verify the value has the correct number of columns
		if self.correctColumnNum != 1 and len(value) == 1:
			# this means we want to clear / delete the key
			del self[key]
		elif self.correctColumnNum > 0:
			if len(value) != self.correctColumnNum:
				if self.strict:
					self.__teePrintOrNot(f"Value {value} does not have the correct number of columns: {self.correctColumnNum}. Refuse adding key...",'error')
					return
				elif self.verbose:
					self.__teePrintOrNot(f"Value {value} does not have the correct number of columns: {self.correctColumnNum}, correcting...",'warning')
				if len(value) < self.correctColumnNum:
					value += ['']*(self.correctColumnNum-len(value))
				elif len(value) > self.correctColumnNum:
					value = value[:self.correctColumnNum]
		else:
			self.correctColumnNum = len(value)
		if self.defaults and len(self.defaults) > 1:
			for i in range(1,len(value)):
				if not value[i] and i < len(self.defaults) and self.defaults[i]:
					value[i] = self.defaults[i]
					if self.verbose:
						self.__teePrintOrNot(f"    Replacing empty value at {i} with default: {self.defaults[i]}")
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = value
			if self.verbose:
				self.__teePrintOrNot(f"Defaults set to {value}")
			if not self.memoryOnly:
				self.appendQueue.append(value)
				self.lastUpdateTime = get_time_ns()
				if self.verbose:
					self.__teePrintOrNot(f"Appending Defaults {key} to the appendQueue")
			return
		if self.verbose:
			self.__teePrintOrNot(f"Setting {key} to {value}")
		if key in self:
			if self[key] == value:
				if self.verbose:
					self.__teePrintOrNot(f"Key {key} already exists with the same value")
				return
			self.dirty = True
		# update the dictionary, 
		super().__setitem__(key,value)
		if self.memoryOnly:
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} updated in memory only")
			return
		elif key.startswith('#'):
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} updated in memory only as it starts with #")
			return
		if self.verbose:
			self.__teePrintOrNot(f"Appending {key} to the appendQueue")
		self.appendQueue.append(value)
		self.lastUpdateTime = get_time_ns()
		# if not self.appendThread.is_alive():
		#     self.commitAppendToFile()
		# else:
		#     self.appendEvent.set()

	def __getitem__(self, key):
		return super().__getitem__(str(key).rstrip())
		

	def __delitem__(self,key):
		key = str(key).rstrip()
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = [DEFAULTS_INDICATOR_KEY]
			if self.verbose:
				self.__teePrintOrNot("Defaults cleared")
			if not self.memoryOnly:
				self.__appendEmptyLine(key)
				if self.verbose:
					self.__teePrintOrNot(f"Appending empty default line {key}")
			return
		# delete the key from the dictionary and update the file
		if key not in self:
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} not found")
			return
		super().__delitem__(key)
		if self.memoryOnly or key.startswith('#'):
			if self.verbose:
				self.__teePrintOrNot(f"Key {key} deleted in memory")
			return
		self.__appendEmptyLine(key)
		if self.verbose:
			self.__teePrintOrNot(f"Appending empty line {key}")
		self.lastUpdateTime = get_time_ns()
		
	def __appendEmptyLine(self,key):
		self.dirty = True
		if self.correctColumnNum > 0:
			emptyLine = [key]+[self.delimiter]*(self.correctColumnNum-1)
		elif len(self[key]) > 1:
			self.correctColumnNum = len(self[key])
			emptyLine = [key]+[self.delimiter]*(self.correctColumnNum-1)
		else:
			emptyLine = [key]
		if self.verbose:
			self.__teePrintOrNot(f"Appending {emptyLine} to the appendQueue")
		self.appendQueue.append(emptyLine)
		return self

	def getListView(self):
		return getListView(self,header=self.header,delimiter=self.delimiter)

	def clear(self):
		# clear the dictionary and update the file
		super().clear()
		if self.verbose:
			self.__teePrintOrNot(f"Clearing {self._fileName}")
		if self.memoryOnly:
			return self
		self.clear_file()
		self.lastUpdateTime = self.externalFileUpdateTime
		return self

	def clear_file(self):
		try:
			if self.header:
				file = self.get_file_obj('wb')
				header = self.delimiter.join(_sanitize(self.header,delimiter=self.delimiter))
				file.write(header.encode(self.encoding,errors='replace') + b'\n')
				self.release_file_obj(file)
				if self.verbose:
					self.__teePrintOrNot(f"Header {header} written to {self._fileName}")
					self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			else:
				file = self.get_file_obj('wb')
				self.release_file_obj(file)
				if self.verbose:
					self.__teePrintOrNot(f"File {self._fileName} cleared empty")
					self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			self.dirty = False
			self.deSynced = False
		except Exception as e:
			self.release_file_obj(file)
			self.__teePrintOrNot(f"Failed to write at clear_file() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
		return self
	
	def __enter__(self):
		return self
	
	def close(self):
		self.stopAppendThread()
		return self

	def __exit__(self,exc_type,exc_value,traceback):
		return self.close()
	
	def __repr__(self):
		return f"""TSVZed(
file_name:{self._fileName}
teeLogger:{self.teeLogger}
header:{self.header}
correctColumnNum:{self.correctColumnNum}
createIfNotExist:{self.createIfNotExist}
verifyHeader:{self.verifyHeader}
rewrite_on_load:{self.rewrite_on_load}
rewrite_on_exit:{self.rewrite_on_exit}
rewrite_interval:{self.rewrite_interval}
append_check_delay:{self.append_check_delay}
appendQueueLength:{len(self.appendQueue)}
appendThreadAlive:{self.appendThread.is_alive()}
dirty:{self.dirty}
deSynced:{self.deSynced}
memoryOnly:{self.memoryOnly}
{dict(self)})"""
	
	def __str__(self):
		return f"TSVZed({self._fileName},{dict(self)})"

	def __del__(self):
		return self.close()

	def popitem(self, last=True):
		key, value = super().popitem(last)
		if not self.memoryOnly:
			self.__appendEmptyLine(key)
		self.lastUpdateTime = get_time_ns()
		return key, value
	
	__marker = object()

	def pop(self, key, default=__marker):
		'''od.pop(k[,d]) -> v, remove specified key and return the corresponding
		value.  If key is not found, d is returned if given, otherwise KeyError
		is raised.

		'''
		key = str(key).rstrip()
		if key not in self:
			if default is self.__marker:
				raise KeyError(key)
			return default
		value = super().pop(key)
		if not self.memoryOnly:
			self.__appendEmptyLine(key)
		self.lastUpdateTime = get_time_ns()
		return value
	
	def move_to_end(self, key, last=True):
		'''Move an existing element to the end (or beginning if last is false).
		Raise KeyError if the element does not exist.
		'''
		key = str(key).rstrip()
		super().move_to_end(key, last)
		self.dirty = True
		if not self.rewrite_on_exit:
			self.rewrite_on_exit = True
			self.__teePrintOrNot("Warning: move_to_end had been called. Need to resync for changes to apply to disk.")
			self.__teePrintOrNot("rewrite_on_exit set to True")
		if self.verbose:
			self.__teePrintOrNot(f"Warning: Trying to move Key {key} moved to {'end' if last else 'beginning'} Need to resync for changes to apply to disk")
		self.lastUpdateTime = get_time_ns()
		return self

	def __sizeof__(self):
		sizeof = sys.getsizeof
		size = sizeof(super()) + sizeof(True) * 12  # for the booleans / integers
		size += sizeof(self.externalFileUpdateTime)
		size += sizeof(self.lastUpdateTime)
		size += sizeof(self._fileName)
		size += sizeof(self.teeLogger)
		size += sizeof(self.delimiter)
		size += sizeof(self.defaults)
		size += sizeof(self.header)
		size += sizeof(self.appendQueue)
		size += sizeof(self.encoding)
		size += sizeof(self.writeLock)
		size += sizeof(self.shutdownEvent)
		size += sizeof(self.appendThread)
		size += super().__sizeof__()
		return size

	@classmethod
	def fromkeys(cls, iterable, value=None,fileName = None,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,verbose = False):
		'''Create a new ordered dictionary with keys from iterable and values set to value.
		'''
		self = cls(fileName,teeLogger,header,createIfNotExist,verifyHeader,rewrite_on_load,rewrite_on_exit,rewrite_interval,append_check_delay,verbose)
		for key in iterable:
			self[key] = value
		return self


	def rewrite(self,force = False,reloadInternalFromFile = None):
		if not self.deSynced and not force:
			if not self.dirty:
				return False
			if self.rewrite_interval == 0 or time.time() - os.path.getmtime(self._fileName) < self.rewrite_interval:
				return False
		try:

			if reloadInternalFromFile is None:
				reloadInternalFromFile = self.monitor_external_changes
			if reloadInternalFromFile and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
				# this will be needed if more than 1 process is accessing the file
				self.commitAppendToFile()
				self.reload()
			if self.memoryOnly:
				if self.verbose:
					self.__teePrintOrNot("Memory only mode. Map to file skipped.")
				return False
			if self.dirty:
				if self.verbose:
					self.__teePrintOrNot(f"Rewriting {self._fileName}")
				self.mapToFile()
				if self.verbose:
					self.__teePrintOrNot(f"{len(self)} records rewrote to {self._fileName}")
			if not self.appendThread.is_alive():
				self.commitAppendToFile()
			# else:
			#     self.appendEvent.set()
			return True
		except Exception as e:
			self.__teePrintOrNot(f"Failed to write at sync() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
			return False
		
	def hardMapToFile(self):
		try:
			if (not self.monitor_external_changes) and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
				self.__teePrintOrNot(f"Warning: Overwriting external changes in {self._fileName}",'warning')
			file = self.get_file_obj('wb')
			buf = io.BufferedWriter(file, buffer_size=64*1024*1024)  # 64MB buffer
			if self.header:
				header = self.delimiter.join(_sanitize(self.header,delimiter=self.delimiter))
				buf.write(header.encode(self.encoding,errors='replace') + b'\n')
			for key in self:
				segments = _sanitize(self[key],delimiter=self.delimiter)
				buf.write(self.delimiter.join(segments).encode(encoding=self.encoding,errors='replace')+b'\n')
			buf.flush()
			self.release_file_obj(file)
			if self.verbose:
				self.__teePrintOrNot(f"{len(self)} records written to {self._fileName}")
				self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			self.dirty = False
			self.deSynced = False
		except Exception as e:
			self.release_file_obj(file)
			self.__teePrintOrNot(f"Failed to write at hardMapToFile() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
		return self
	
	def mapToFile(self):
		mec = self.monitor_external_changes
		self.monitor_external_changes = False
		try:
			if (not self.monitor_external_changes) and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
				self.__teePrintOrNot(f"Warning: Overwriting external changes in {self._fileName}",'warning')
			if self._fileName.rpartition('.')[2] in COMPRESSED_FILE_EXTENSIONS:
				# if the file is compressed, we need to use the hardMapToFile method
				return self.hardMapToFile()
			file = self.get_file_obj('r+b')
			overWrite = False
			if self.header:
				line = file.readline().decode(self.encoding,errors='replace')
				aftPos = file.tell()
				if not _lineContainHeader(self.header,line,verbose = self.verbose,teeLogger = self.teeLogger,strict = self.strict):
					header = self.delimiter.join(_sanitize(self.header,delimiter=self.delimiter))
					file.seek(0)
					file.write(f'{header}\n'.encode(encoding=self.encoding,errors='replace'))
					# if the header is not the same length as the line, we need to overwrite the file
					if aftPos != file.tell():
						overWrite = True
					if self.verbose:
						self.__teePrintOrNot(f"Header {header} written to {self._fileName}")
			for value in self.values():
				if value[0].startswith('#'):
					continue
				segments = _sanitize(value,delimiter=self.delimiter)
				strToWrite = self.delimiter.join(segments)
				if overWrite:
					if self.verbose:
						self.__teePrintOrNot(f"Overwriting {value} to {self._fileName}")
					file.write(strToWrite.encode(encoding=self.encoding,errors='replace')+b'\n')
					continue
				pos = file.tell()
				line = file.readline()
				aftPos = file.tell()
				if not line or pos == aftPos:
					if self.verbose:
						self.__teePrintOrNot(f"End of file reached. Appending {value} to {self._fileName}")
					file.write(strToWrite.encode(encoding=self.encoding,errors='replace'))
					overWrite = True
					continue
				strToWrite = strToWrite.encode(encoding=self.encoding,errors='replace').ljust(len(line)-1)+b'\n'
				if line != strToWrite:
					if self.verbose:
						self.__teePrintOrNot(f"Modifing {value} to {self._fileName}")
					file.seek(pos)
					# fill the string with space to write to the correct length
					file.write(strToWrite)
					if aftPos != file.tell():
						overWrite = True
			file.truncate()
			self.release_file_obj(file)
			if self.verbose:
				self.__teePrintOrNot(f"{len(self)} records written to {self._fileName}")
				self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
			self.dirty = False
			self.deSynced = False
		except Exception as e:
			self.release_file_obj(file)
			self.__teePrintOrNot(f"Failed to write at mapToFile() to {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
			self.deSynced = True
			self.__teePrintOrNot("Trying failback hardMapToFile()")
			self.hardMapToFile()
		self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)
		self.monitor_external_changes = mec
		return self
	
	def checkExternalChanges(self):
		if self.deSynced:
			return self
		if not self.monitor_external_changes:
			return self
		realExternalFileUpdateTime = getFileUpdateTimeNs(self._fileName)
		if self.externalFileUpdateTime < realExternalFileUpdateTime:
			self.deSynced = True
			self.__teePrintOrNot(f"External changes detected in {self._fileName}")
		elif self.externalFileUpdateTime > realExternalFileUpdateTime:
			self.__teePrintOrNot(f"Time anomalies detected in {self._fileName}, resetting externalFileUpdateTime")
			self.externalFileUpdateTime = realExternalFileUpdateTime
		return self

	def _appendWorker(self):
		while not self.shutdownEvent.is_set():
			if not self.memoryOnly:
				self.checkExternalChanges()
				self.rewrite()
				self.commitAppendToFile()
			time.sleep(self.append_check_delay)
			# self.appendEvent.wait()
			# self.appendEvent.clear()
		if self.verbose:
			self.__teePrintOrNot(f"Append worker for {self._fileName} shut down")
		self.commitAppendToFile()

	def commitAppendToFile(self):
		if self.appendQueue:
			if self.memoryOnly:
				self.appendQueue.clear()
				if self.verbose:
					self.__teePrintOrNot("Memory only mode. Append queue cleared.") 
				return self
			try:
				if self.verbose:
					self.__teePrintOrNot(f"Commiting {len(self.appendQueue)} records to {self._fileName}")
					self.__teePrintOrNot(f"Before size of {self._fileName}: {os.path.getsize(self._fileName)}")
				file = self.get_file_obj('ab')
				buf = io.BufferedWriter(file, buffer_size=64*1024*1024)  # 64MB buffer
				while self.appendQueue:
					line = _sanitize(self.appendQueue.popleft(),delimiter=self.delimiter)
					buf.write(self.delimiter.join(line).encode(encoding=self.encoding,errors='replace')+b'\n')
				buf.flush()
				self.release_file_obj(file)
				if self.verbose:
					self.__teePrintOrNot(f"Records commited to {self._fileName}")
					self.__teePrintOrNot(f"After size of {self._fileName}: {os.path.getsize(self._fileName)}")
			except Exception as e:
				self.release_file_obj(file)
				self.__teePrintOrNot(f"Failed to write at commitAppendToFile to {self._fileName}: {e}",'error')
				import traceback
				self.__teePrintOrNot(traceback.format_exc(),'error')
				self.deSynced = True
		return self
	
	def stopAppendThread(self):
		try:
			if self.shutdownEvent.is_set():
				# if self.verbose:
				#     self.__teePrintOrNot(f"Append thread for {self._fileName} already stopped")
				return
			self.rewrite(force=self.rewrite_on_exit)  # Ensure any final sync operations are performed
			# self.appendEvent.set()
			self.shutdownEvent.set()  # Signal the append thread to shut down
			self.appendThread.join()  # Wait for the append thread to complete 
			if self.verbose:
				self.__teePrintOrNot(f"Append thread for {self._fileName} stopped")
		except Exception as e:
			self.__teePrintOrNot(f"Failed to stop append thread for {self._fileName}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
	
	def get_file_obj(self,modes = 'ab'):
		self.writeLock.acquire()
		try:
			if not self.encoding:
				self.encoding = 'utf8'
			file = openFileAsCompressed(self._fileName, mode=modes, encoding=self.encoding,teeLogger=self.teeLogger)
			# Lock the file after opening
			if os.name == 'posix':
				fcntl.lockf(file, fcntl.LOCK_EX)
			elif os.name == 'nt':
				# For Windows, locking the entire file, avoiding locking an empty file
				#lock_length = max(1, os.path.getsize(self._fileName))
				lock_length = 2147483647
				msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, lock_length)
			if self.verbose:
				self.__teePrintOrNot(f"File {self._fileName} locked with mode {modes}")
		except Exception as e:
			try:
				self.writeLock.release()  # Release the thread lock in case of an error
			except Exception as e:
				self.__teePrintOrNot(f"Failed to release writeLock for {self._fileName}: {e}",'error')
			self.__teePrintOrNot(f"Failed to open file {self._fileName}: {e}",'error')
		return file

	def release_file_obj(self,file):
		# if write lock is already released, return
		if not self.writeLock.locked():
			return
		try:
			file.flush()  # Ensure the file is flushed before unlocking
			os.fsync(file.fileno())  # Ensure the file is synced to disk before unlocking
			if not file.closed:
				if os.name == 'posix':
					fcntl.lockf(file, fcntl.LOCK_UN)
				elif os.name == 'nt':
					# Unlocking the entire file; for Windows, ensure not unlocking an empty file
					#unlock_length = max(1, os.path.getsize(os.path.realpath(file.name)))
					unlock_length = 2147483647
					try:
						msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, unlock_length)
					except Exception:
						pass
				file.close()  # Ensure file is closed after unlocking
			if self.verbose:
				self.__teePrintOrNot(f"File {file.name} unlocked / released")
		except Exception as e:
			try:
				self.writeLock.release()  # Ensure the thread lock is always released
			except Exception as e:
				self.__teePrintOrNot(f"Failed to release writeLock for {file.name}: {e}",'error')
			self.__teePrintOrNot(f"Failed to release file {file.name}: {e}",'error')
			import traceback
			self.__teePrintOrNot(traceback.format_exc(),'error')
		# release the write lock if not already released
		if self.writeLock.locked():
			try:
				self.writeLock.release()  # Ensure the thread lock is always released
			except Exception as e:
				self.__teePrintOrNot(f"Failed to release writeLock for {file.name}: {e}",'error')
			self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)

class TSVZedLite(MutableMapping):
	"""
	A mutable mapping class that provides a dictionary-like interface to a Tabular (TSV by default) file.
	TSVZedLite stores key-value pairs where each row in the file represents an entry, with the first
	column serving as the key. The class maintains an in-memory index of file positions for efficient
	random access while keeping the actual data on disk.
	TSVZedLite is designed for light memory footprint and forgoes some features from TSVZed, Notably,
	- Does not support simultaneous multi-process access.
	- Does not support compressed file formats.
	- Does not support automatic file rewriting on load / exit / periodically.
	- Does not support append worker thread for background writes.
	- Does not support external file change monitoring.
	- Does not support in-place updates; updates are append-only.
	- Does not support logging via teeLogger.
	- Does not support move_to_end method.
	- Does not support in-memory only mode. ( please just use a dict )
	- Does not lock the file during operations.
	- Does not track last update times.
	
	However, it may be preferred in scenarios when:
	- Memory usage needs to be minimized.
	- Working with extremely large datasets where loading everything into memory is impractical.
	- Simplicity and ease of use are prioritized over advanced features.
	- The dataset is primarily write-only with infrequent reads.
	- The application can tolerate the lack of concurrency control. (single process access only)
	- Underlying file system is fast and can do constant time random seek (e.g., SSD).

	Note: It is possible to load a custom dict like object for indexes (like TSVZed or pre-built dict)
	to avoid reading the entire data file to load the indexes at startup. 
	Index consistency is not enforced in this case.
	Will raise error if mismatch happen (only checkes key exist in file) and strict mode is enabled.
	If using an external file-backed Index. This can function similar to a key-value store (like nosql).

	Parameters
	----------
	fileName : str
		Path to the Tabular file to read from or create.
	header : str, optional
		Header row for the file. Can be a delimited string or empty string (default: '').
	createIfNotExist : bool, optional
		If True, creates the file if it doesn't exist (default: True).
	verifyHeader : bool, optional
		If True, verifies that the file header matches the provided header (default: True).
	verbose : bool, optional
		If True, prints detailed operation information to stderr (default: False).
	encoding : str, optional
		Character encoding for the file (default: 'utf8').
	delimiter : str, optional
		Field delimiter character. If Ellipsis (...), automatically detects from filename (default: ...).
	defaults : str, list, or None, optional
		Default values for columns. Can be a delimited string, list, or None (default: None).
	strict : bool, optional
		If True, enforces strict column count validation and raises errors on mismatches (default: True).
	correctColumnNum : int, optional
		Expected number of columns. -1 means auto-detect (default: -1).
	indexes : dict, optional
		Pre-existing index dictionary mapping keys to file positions (default: ...).
	fileObj : file object, optional
		Pre-existing file object to use (default: ...).
	Attributes
	----------
	version : str
		Version identifier for the TSVZedLite format.
	indexes : dict
		Dictionary mapping keys to their file positions (or in-memory data for keys starting with '#').
	fileObj : file object
		Binary file object for reading/writing the underlying file.
	defaults : list
		List of default values for columns, with DEFAULTS_INDICATOR_KEY as the first element.
	correctColumnNum : int
		The validated number of columns per row.
	Notes
	-----
	- Keys starting with '#' are stored in memory only and not written to file.
	- The special key DEFAULTS_INDICATOR_KEY is used to store and retrieve default column values.
	- Empty values in rows are automatically filled with defaults if available.
	- The class implements the MutableMapping interface, providing dict-like operations.
	- File operations are buffered and written immediately (append-only for updates).
	- Deleted entries are marked by writing a row with only the key (empty values).
	Examples
	--------
	>>> db = TSVZedLite('data.tsv', header='id\tname\tage')
	>>> db['user1'] = ['user1', 'Alice', '30']
	>>> print(db['user1'])
	['user1', 'Alice', '30']
	>>> del db['user1']
	>>> 'user1' in db
	False
	See Also
	--------
	collections.abc.MutableMapping : The abstract base class that this class implements.
	"""

	#['__new__', '__repr__', '__hash__', '__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__', '__iter__', '__init__',
	#  '__or__', '__ror__', '__ior__', '__len__', '__getitem__', '__setitem__', '__delitem__', '__contains__', '__sizeof__',
	#  'get', 'setdefault', 'pop', 'popitem', 'keys', 'items', 'values', 'update', 'fromkeys', 'clear', 'copy', '__reversed__',
	#  '__class_getitem__', '__doc__']
	def __init__ (self,fileName,header = '',createIfNotExist = True,verifyHeader = True,
					verbose = False,encoding = 'utf8',
					delimiter = ...,defaults = None,strict = True,correctColumnNum = -1,
					indexes = ..., fileObj = ...
					):
		self.version = version
		self.strict = strict
		self._fileName = fileName
		self.delimiter = get_delimiter(delimiter,file_name=fileName)
		self.setDefaults(defaults)
		self.header = _formatHeader(header,verbose = verbose,delimiter=self.delimiter)
		self.correctColumnNum = correctColumnNum
		self.createIfNotExist = createIfNotExist
		self.verifyHeader = verifyHeader
		self.verbose = verbose
		self.encoding = encoding
		if indexes is ...:
			self.indexes = dict()
			self.load()
		else:
			self.indexes = indexes
		if fileObj is ...:
			self.fileObj = open(self._fileName,'r+b')
		else:
			self.fileObj = fileObj
		atexit.register(self.close)

	# Implement custom methods just for TSVZedLite
	def getResourceUsage(self,return_dict = False):
		return get_resource_usage(return_dict = return_dict)

	def setDefaults(self,defaults):
		if not defaults:
			defaults = []
		if isinstance(defaults,str):
			defaults = defaults.split(self.delimiter)
		elif not isinstance(defaults,list):
			try:
				defaults = list(defaults)
			except Exception:
				if self.verbose:
					eprint('Error: Invalid defaults, setting defaults to empty.')
				defaults = []
		defaults = [str(s).rstrip() if s else '' for s in defaults]
		if not any(defaults):
			defaults = []
		if not defaults or defaults[0] != DEFAULTS_INDICATOR_KEY:
			defaults = [DEFAULTS_INDICATOR_KEY]+defaults
		self.defaults = defaults

	def load(self):
		if self.verbose:
			eprint(f"Loading {self._fileName}")
		readTabularFile(self._fileName, header = self.header, createIfNotExist = self.createIfNotExist,
				   verifyHeader = self.verifyHeader, verbose = self.verbose, taskDic = self.indexes,
				   encoding = self.encoding if self.encoding else None, strict = self.strict, 
				   delimiter = self.delimiter, defaults=self.defaults,storeOffset=True)
		return self

	def positions(self):
		return self.indexes.values()

	def reload(self):
		self.indexes.clear()
		return self.load()

	def getListView(self):
		return getListView(self,header=self.header,delimiter=self.delimiter)

	def clear_file(self):
		if self.verbose:
			eprint(f"Clearing {self._fileName}")
		self.fileObj.seek(0)
		self.fileObj.truncate()
		if self.verbose:
			eprint(f"File {self._fileName} cleared empty")
		if self.header:
			location = self.__writeValues(self.header)
			if self.verbose:
				eprint(f"Header {self.header} written to {self._fileName}")
				eprint(f"At {location} size: {self.fileObj.tell()}")
		return self
	
	def switchFile(self,newFileName,createIfNotExist = ...,verifyHeader = ...):
		if createIfNotExist is ...:
			createIfNotExist = self.createIfNotExist
		if verifyHeader is ...:
			verifyHeader = self.verifyHeader
		self.fileObj.close()
		self._fileName = newFileName
		self.reload()
		self.fileObj = open(self._fileName,'r+b')
		self.createIfNotExist = createIfNotExist
		self.verifyHeader = verifyHeader
		return self

	# Private methods for reading and writing values for TSVZedLite

	def __writeValues(self,data):
		self.fileObj.seek(0, os.SEEK_END)
		write_at = self.fileObj.tell()
		if self.verbose:
			eprint(f"Writing at position {write_at}")
		data = _sanitize(data,delimiter=self.delimiter)
		data = self.delimiter.join(data)
		bytes = self.fileObj.write((data.encode(encoding=self.encoding,errors='replace') + b'\n'))
		if self.verbose:
			eprint(f"Wrote {bytes} bytes")
		return write_at

	def __mapDeleteToFile(self,key):
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = [DEFAULTS_INDICATOR_KEY]
			if self.verbose:
				eprint("Defaults cleared")
		# delete the key from the dictionary and update the file
		elif key not in self.indexes:
			if self.verbose:
				eprint(f"Key {key} not found")
			return
		elif key.startswith('#'):
			if self.verbose:
				eprint(f"Key {key} deleted in memory")
			return
		if self.verbose:
			eprint(f"Appending empty line {key}")
		self.indexes[key] = self.__writeValues([key])

	def __readValuesAtPos(self,pos,key = ...):
		self.fileObj.seek(pos)
		line = self.fileObj.readline().decode(self.encoding,errors='replace')
		self.correctColumnNum, segments = _processLine(
						line=line,
						taskDic={},
						correctColumnNum=self.correctColumnNum,
						strict=self.strict,
						delimiter=self.delimiter,
						defaults=self.defaults,
						storeOffset=True,
					)
		if self.verbose:
			eprint(f"Read at position {pos}: {segments}")
		if key is not ... and segments[0] != key:
				eprint(f"Warning: Key mismatch at position {pos}: expected {key}, got {segments[0]}")
				if self.strict:
					eprint("Error: Key mismatch and strict mode enabled. Raising KeyError.")
					raise KeyError(key)
				else :
					eprint("Continuing despite key mismatch due to non-strict mode. Expect errors!")
		return segments

	# Implement basic __getitem__, __setitem__, __delitem__, __iter__, and __len__. needed for MutableMapping
	def __getitem__(self,key):
		key = str(key).rstrip()
		if key not in self.indexes:
			if key == DEFAULTS_INDICATOR_KEY:
				return self.defaults
			raise KeyError(key)
		pos = self.indexes[key]
		return self.__readValuesAtPos(pos,key)

	def __setitem__(self,key,value):
		key = str(key).rstrip()
		if not key:
			eprint('Error: Key cannot be empty')
			return
		if isinstance(value,str):
			value = value.split(self.delimiter)
		# sanitize the value
		value = [str(s).rstrip() if s else '' for s in value]
		# the first field in value should be the key
		# add it if it is not there
		if not value or value[0] != key:
			value = [key]+value
		# verify the value has the correct number of columns
		if self.correctColumnNum != 1 and len(value) == 1:
			# this means we want to clear / delete the key
			del self[key]
		elif self.correctColumnNum > 0:
			if len(value) != self.correctColumnNum:
				if self.strict:
					eprint(f"Error: Value {value} does not have the correct number of columns: {self.correctColumnNum}. Refuse adding key...")
					return
				elif self.verbose:
					eprint(f"Warning: Value {value} does not have the correct number of columns: {self.correctColumnNum}, correcting...")
				if len(value) < self.correctColumnNum:
					value += ['']*(self.correctColumnNum-len(value))
				elif len(value) > self.correctColumnNum:
					value = value[:self.correctColumnNum]
		else:
			self.correctColumnNum = len(value)
		if self.defaults and len(self.defaults) > 1:
			for i in range(1,len(value)):
				if not value[i] and i < len(self.defaults) and self.defaults[i]:
					value[i] = self.defaults[i]
					if self.verbose:
						eprint(f"    Replacing empty value at {i} with default: {self.defaults[i]}")
		if key == DEFAULTS_INDICATOR_KEY:
			self.defaults = value
			if self.verbose:
				eprint(f"Defaults set to {value}")
		elif key.startswith('#'):
			if self.verbose:
				eprint(f"Key {key} updated in memory (data in index) as it starts with #")
			self.indexes[key] = value
			return
		if self.verbose:
			eprint(f"Writing {key}: {value}")
		self.indexes[key] = self.__writeValues(value)
		
	def __delitem__(self,key):
		key = str(key).rstrip()
		self.indexes.pop(key,None)
		self.__mapDeleteToFile(key)

	def __iter__(self):
		return iter(self.indexes)

	def __len__(self):
		return len(self.indexes)

	# Implement additional methods for dict like interface (order of function are somewhat from OrderedDict)
	def __reversed__(self):
		return reversed(self.indexes)

	def clear(self):
		# clear the dictionary and update the file
		self.indexes.clear()
		self.clear_file()
		return self

	def popitem(self, last=True,return_pos = False):
		if last:
			key, pos = self.indexes.popitem()
		else:
			try:
				key = next(iter(self.indexes))
				pos = self.indexes.pop(key)
			except StopIteration:
				raise KeyError("popitem(): dictionary is empty")
		if return_pos:
			value = pos
		else:
			value = self.__readValuesAtPos(pos,key)
		self.__mapDeleteToFile(key)
		return key, value

	__marker = object()
	def pop(self, key, default=__marker, return_pos = False):
		key = str(key).rstrip()
		try:
			pos = self.indexes.pop(key)
		except KeyError:
			if default is self.__marker:
				raise KeyError(key)
			elif default is ...:
				return self.defaults
			return default
		if return_pos:
			value = pos
		else:
			value = self.__readValuesAtPos(pos,key)
		self.__mapDeleteToFile(key)
		return value

	def __sizeof__(self):
		sizeof = sys.getsizeof
		size = sizeof(super()) + sizeof(True) * 6  # for the booleans / integers
		size += sizeof(self._fileName)
		size += sizeof(self.header)
		size += sizeof(self.encoding)
		size += sizeof(self.delimiter)
		size += sizeof(self.defaults)
		size += sizeof(self.indexes)
		size += sizeof(self.fileObj)
		return size
	
	def __repr__(self):
		return f"""TSVZed at {hex(id(self))}(
file_name:{self._fileName}
index_count:{len(self.indexes)}
header:{self.header}
correctColumnNum:{self.correctColumnNum}
createIfNotExist:{self.createIfNotExist}
verifyHeader:{self.verifyHeader}
strict:{self.strict}
delimiter:{self.delimiter}
defaults:{self.defaults}
verbose:{self.verbose}
encoding:{self.encoding}
file_descriptor:{self.fileObj.fileno()}
)"""

	def __str__(self):
		return f"TSVZedLite({self._fileName})"
	
	def __reduce__(self):
		'Return state information for pickling'
		# Return minimal state needed to reconstruct
		return (
			self.__class__,
			(self._fileName, self.header, self.createIfNotExist, self.verifyHeader,
			 self.verbose, self.encoding, self.delimiter, self.defaults, self.strict, 
			 self.correctColumnNum),
			None,
			None,
			None
		)
	def copy(self):
		'Return a shallow copy of the ordered dictionary.'
		new = self.__class__(
			self._fileName,
			self.header,
			self.createIfNotExist,
			self.verifyHeader,
			self.verbose,
			self.encoding,
			self.delimiter,
			self.defaults,
			self.strict,
			self.correctColumnNum,
			self.indexes,
			self.fileObj,
		)
		eprint("""
		Warning: Copying TSVZedLite will share the same file object and indexes. 
		Changes in one will affect the other.
		There is likely very little reason to copy a TSVZedLite instance unless you are immadiately then calling switchFile() on it.
		""")
		return new

	@classmethod
	def fromkeys(cls, iterable, value=None,fileName = None,header = '',createIfNotExist = True,verifyHeader = True,verbose = False,encoding = 'utf8',
					delimiter = ...,defaults = None,strict = True,correctColumnNum = -1):
		'''Create a new ordered dictionary with keys from iterable and values set to value.
		'''
		self = cls(fileName,header,createIfNotExist,verifyHeader,verbose,encoding,delimiter,defaults,strict,correctColumnNum)
		for key in iterable:
			self[key] = value
		return self
	
	def __eq__(self, other):
		if isinstance(other, TSVZedLite):
			eprint("Warning: Comparing two TSVZedLite instances will only compare their indexes. Data content is not compared.")
			return self.indexes == other.indexes
		return super().__eq__(other)
	
	def __ior__(self, other):
		self.update(other)
		return self

	# Implement context manager methods
	def __enter__(self):
		return self
	
	def close(self):
		self.fileObj.close()
		return self

	def __exit__(self,exc_type,exc_value,traceback):
		return self.close()




def __main__():
	import argparse
	parser = argparse.ArgumentParser(description='TSVZed: A TSV / CSV / NSV file manager')
	parser.add_argument('filename', type=str, help='The file to read')
	parser.add_argument('operation', type=str,nargs='?', choices=['read','append','delete','clear','scrub'], help='The operation to perform. Note: scrub will also remove all comments. Default: read', default='read')
	parser.add_argument('line', type=str, nargs='*', help='The line to append to the Tabular file. it follows as : {key} {value1} {value2} ... if a key without value be inserted, the value will get deleted.')
	parser.add_argument('-d', '--delimiter', type=str, help='The delimiter of the Tabular file. Default: Infer from last part of filename, or tab if cannot determine. Note: accept unicode escaped char, raw char, or string "comma,tab,null" will refer to their characters. ', default=...)
	parser.add_argument('-c', '--header', type=str, help='Perform checks with this header of the Tabular file. seperate using --delimiter.')
	parser.add_argument('--defaults', type=str, help='Default values to fill in the missing columns. seperate using --delimiter. Ex. if -d = comma, --defaults="key,value1,value2..." Note: Please specify the key. But it will not be used as a key need to be unique in data.')
	strictMode = parser.add_mutually_exclusive_group()
	strictMode.add_argument('-s', '--strict', dest = 'strict',action='store_true', help='Strict mode. Do not parse values that seems malformed, check for column numbers / headers')
	strictMode.add_argument('-f', '--force', dest = 'strict',action='store_false', help='Force the operation. Ignore checks for column numbers / headers')
	parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output')
	parser.add_argument('-V', '--version', action='version', version=f'%(prog)s {version} @ {COMMIT_DATE} by {author}')
	args = parser.parse_args()
	args.delimiter = get_delimiter(delimiter=args.delimiter,file_name=args.filename)
	if args.header and args.header.endswith('\\'):
		args.header += '\\'
	try:
		header = args.header.encode().decode('unicode_escape') if args.header else ''
	except Exception:
		print(f"Failed to decode header: {args.header}")
		header = ''
	defaults = []
	if args.defaults:
		try:
			defaults = args.defaults.encode().decode('unicode_escape').split(args.delimiter)
		except Exception:
			print(f"Failed to decode defaults: {args.defaults}")
			defaults = []

	if args.operation == 'read':
		# check if the file exist
		if not os.path.isfile(args.filename):
			print(f"File not found: {args.filename}")
			return
		# read the file
		data = readTabularFile(args.filename, verifyHeader = False, verbose=args.verbose,strict= args.strict, delimiter=args.delimiter, defaults=defaults)
		print(pretty_format_table(data.values(),delimiter=args.delimiter))
	elif args.operation == 'append':
		appendTabularFile(args.filename, args.line,createIfNotExist = True, header=header, verbose=args.verbose, strict= args.strict, delimiter=args.delimiter)
	elif args.operation == 'delete':
		appendTabularFile(args.filename, args.line[:1],createIfNotExist = True, header=header, verbose=args.verbose, strict= args.strict, delimiter=args.delimiter)
	elif args.operation == 'clear':
		clearTabularFile(args.filename, header=header, verbose=args.verbose, verifyHeader=args.strict, delimiter=args.delimiter)
	elif args.operation == 'scrub':
		scrubTabularFile(args.filename, verifyHeader = False, verbose=args.verbose,strict= args.strict, delimiter=args.delimiter, defaults=defaults)
	else:
		print("Invalid operation")    
if __name__ == '__main__':
	__main__()
