#!/usr/bin/env python3
# /// script
# requires-python = ">=3.6"
# dependencies = [
# ]
# ///
import os , sys
from collections import OrderedDict , deque
import time
import atexit
import threading
import re

RESOURCE_LIB_AVAILABLE = True
try:
    import resource
except:
    RESOURCE_LIB_AVAILABLE = False

if os.name == 'nt':
    import msvcrt
elif os.name == 'posix':
    import fcntl

version = '3.24'
__version__ = version
author = 'pan@zopyr.us'

DEFAULT_DELIMITER = '\t'
DEFAULTS_INDICATOR_KEY = '#_defaults_#'

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

def pretty_format_table(data, delimiter = DEFAULT_DELIMITER,header = None):
    version = 1.11
    _ = version
    if not data:
        return ''
    if isinstance(data, str):
        data = data.strip('\n').split('\n')
        data = [line.split(delimiter) for line in data]
    elif isinstance(data, dict):
        # flatten the 2D dict to a list of lists
        if isinstance(next(iter(data.values())), dict):
            tempData = [['key'] + list(next(iter(data.values())).keys())]
            tempData.extend( [[key] + list(value.values()) for key, value in data.items()])
            data = tempData
        else:
            # it is a dict of lists
            data = [[key] + list(value) for key, value in data.items()]
    elif not isinstance(data,list):
        data = list(data)
    # format the list into 2d list of list of strings
    if isinstance(data[0], dict):
        tempData = [data[0].keys()]
        tempData.extend([list(item.values()) for item in data])
        data = tempData
    data = [[str(item) for item in row] for row in data]
    num_cols = len(data[0])
    col_widths = [0] * num_cols
    # Calculate the maximum width of each column
    for c in range(num_cols):
        #col_widths[c] = max(len(row[c]) for row in data)
        # handle ansii escape sequences
        col_widths[c] = max(len(re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',row[c])) for row in data)
    if header:
        header_widths = [len(re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', col)) for col in header]
        col_widths = [max(col_widths[i], header_widths[i]) for i in range(num_cols)]
    # Build the row format string
    row_format = ' | '.join('{{:<{}}}'.format(width) for width in col_widths)
    # Print the header
    if not header:
        header = data[0]
        outTable = []
        outTable.append(row_format.format(*header))
        outTable.append('-+-'.join('-' * width for width in col_widths))
        for row in data[1:]:
            # if the row is empty, print an divider
            if not any(row):
                outTable.append('-+-'.join('-' * width for width in col_widths))
            else:
                outTable.append(row_format.format(*row))
    else:
        # pad / truncate header to appropriate length
        if isinstance(header,str):
            header = header.split(delimiter)
        if len(header) < num_cols:
            header += ['']*(num_cols-len(header))
        elif len(header) > num_cols:
            header = header[:num_cols]
        outTable = []
        outTable.append(row_format.format(*header))
        outTable.append('-+-'.join('-' * width for width in col_widths))
        for row in data:
            # if the row is empty, print an divider
            if not any(row):
                outTable.append('-+-'.join('-' * width for width in col_widths))
            else:
                outTable.append(row_format.format(*row))
    return '\n'.join(outTable) + '\n'

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
			except Exception as e:
				return 0
	elif to_str or isinstance(size, int) or isinstance(size, float):
		if isinstance(size, str):
			try:
				size = size.rstrip('B').rstrip('b')
				size = float(size.lower().strip())
			except Exception as e:
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
            except:
                teeLogger.teelog(message,level)
        else:
            print(message,flush=True)
    except Exception:
        print(message,flush=True)

def _processLine(line,taskDic,correctColumnNum,verbose = False,teeLogger = None,strict = True,delimiter = DEFAULT_DELIMITER,defaults = None):
    """
    Process a line of text and update the task dictionary.

    Parameters:
    line (str): The line of text to process.
    taskDic (dict): The dictionary to update with the processed line.
    correctColumnNum (int): The expected number of columns in the line.
    verbose (bool, optional): Whether to print verbose output. Defaults to False.
    teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
    strict (bool, optional): Whether to strictly enforce the correct number of columns. Defaults to True.
    defaults (list, optional): The default values to use for missing columns. Defaults to [].

    Returns:
    tuple: A tuple containing the updated correctColumnNum and the processed lineCache.

    """
    if not defaults:
        defaults = []
    line = line.strip(' ').strip('\x00').rstrip('\r\n')
    # we throw away the lines that start with '#'
    if not line :
        if verbose:
            __teePrintOrNot(f"Ignoring empty line: {line}",teeLogger=teeLogger)
        return correctColumnNum , []
    if line.startswith('#') and not line.startswith(DEFAULTS_INDICATOR_KEY):
        if verbose:
            __teePrintOrNot(f"Ignoring comment line: {line}",teeLogger=teeLogger)
        return correctColumnNum , []
    # we only interested in the lines that have the correct number of columns
    lineCache = [segment.rstrip() for segment in line.split(delimiter)]
    if not lineCache:
        return correctColumnNum , []
    if correctColumnNum == -1:
        if defaults and len(defaults) > 1:
            correctColumnNum = len(defaults)
        else:
            correctColumnNum = len(lineCache)
        if verbose:
            __teePrintOrNot(f"detected correctColumnNum: {len(lineCache)}",teeLogger=teeLogger)
    if not lineCache[0]:
        if verbose:
            __teePrintOrNot(f"Ignoring line with empty key: {line}",teeLogger=teeLogger)
        return correctColumnNum , []
    if len(lineCache) == 1 or not any(lineCache[1:]):
        if correctColumnNum == 1: 
            taskDic[lineCache[0]] = lineCache
        elif lineCache[0] == DEFAULTS_INDICATOR_KEY:
            if verbose:
                __teePrintOrNot(f"Empty defaults line found: {line}",teeLogger=teeLogger)
            defaults.clear()
        else:
            if verbose:
                __teePrintOrNot(f"Key {lineCache[0]} found with empty value, deleting such key's representaion",teeLogger=teeLogger)
            if lineCache[0] in taskDic:
                del taskDic[lineCache[0]]
        return correctColumnNum , []
    elif len(lineCache) != correctColumnNum:
        if strict and not any(defaults):
            if verbose:
                __teePrintOrNot(f"Ignoring line with {len(lineCache)} columns: {line}",teeLogger=teeLogger)
            return correctColumnNum , []
        else:
            # fill / cut the line with empty entries til the correct number of columns
            if len(lineCache) < correctColumnNum:
                lineCache += ['']*(correctColumnNum-len(lineCache))
            elif len(lineCache) > correctColumnNum:
                lineCache = lineCache[:correctColumnNum]
            if verbose:
                __teePrintOrNot(f"Correcting {lineCache[0]}",teeLogger=teeLogger)
    # now replace empty values with defaults
    if defaults and len(defaults) > 1:
        for i in range(1,len(lineCache)):
            if not lineCache[i] and i < len(defaults) and defaults[i]:
                lineCache[i] = defaults[i]
                if verbose:
                    __teePrintOrNot(f"Replacing empty value at {i} with default: {defaults[i]}",teeLogger=teeLogger)
    if lineCache[0] == DEFAULTS_INDICATOR_KEY:
        if verbose:
            __teePrintOrNot(f"Defaults line found: {line}",teeLogger=teeLogger)
        defaults[:] = lineCache
        return correctColumnNum , []
    taskDic[lineCache[0]] = lineCache
    if verbose:
        __teePrintOrNot(f"Key {lineCache[0]} added",teeLogger=teeLogger)
    return correctColumnNum, lineCache

def read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None, strict=False,encoding = 'utf8',delimiter = ...,defaults = []):
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

    Returns:
        list: The last valid line data processed by processLine, or an empty list if none found.
    """
    chunk_size = 1024  # Read in chunks of 1024 bytes
    last_valid_line = []
    delimiter = get_delimiter(delimiter,file_name=fileName)
    if verbose:
        __teePrintOrNot(f"Reading last line only from {fileName}",teeLogger=teeLogger)
    with open(fileName, 'rb') as file:
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        buffer = b''
        position = file_size

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
                if lines[i].strip():  # Skip empty lines
                    # Process the line
                    correctColumnNum, lineCache = _processLine(
                        line=lines[i].decode(encoding=encoding),
                        taskDic=taskDic,
                        correctColumnNum=correctColumnNum,
                        verbose=verbose,
                        teeLogger=teeLogger,
                        strict=strict,
                        delimiter=delimiter,
                        defaults=defaults,
                    )
                    # If the line is valid, return it
                    if lineCache and any(lineCache):
                        return lineCache
            
            # Keep the last (possibly incomplete) line in buffer for the next read
            buffer = lines[0]

    # Return empty list if no valid line found
    return last_valid_line

def _formatHeader(header,verbose = False,teeLogger = None,delimiter = DEFAULT_DELIMITER):
    """
    Format the header string.

    Parameters:
    - header (str or list): The header string or list to format.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.
    - teeLogger (object, optional): The tee logger object for printing output. Defaults to None.

    Returns:
        str: The formatted header string.
    """
    if not isinstance(header,str):
        try:
            header = delimiter.join(header)
        except:
            if verbose:
                __teePrintOrNot('Invalid header, setting header to empty.','error',teeLogger=teeLogger)
            header = ''
    header = delimiter.join([segment.rstrip() for segment in header.split(delimiter)])
    # if header:
    #     if not header.endswith('\n'):
    #         header += '\n'
    # else:
    #     header = ''
    return header

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
    header = [segment.rstrip() for segment in header.split(delimiter)]
    line = [segment.rstrip() for segment in line.split(delimiter)]
    if verbose:
        __teePrintOrNot(f"Header: \n{header}",teeLogger=teeLogger)
        __teePrintOrNot(f"First line: \n{line}",teeLogger=teeLogger)
    if len(header) != len(line) or any([header[i] not in line[i] for i in range(len(header))]):
        __teePrintOrNot(f"Header mismatch: \n{line} \n!= \n{header}",teeLogger=teeLogger)
        if strict:
            raise ValueError("Data format error! Header mismatch")
        return False
    return True

def _verifyFileExistence(fileName,createIfNotExist = True,teeLogger = None,header = '',encoding = 'utf8',strict = True,delimiter = DEFAULT_DELIMITER):
    """
    Verify the existence of the tabular file.

    Parameters:
    - fileName (str): The path of the tabular file.
    - createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to True.
    - teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
    - header (str, optional): The header line to verify against. Defaults to ''.
    - encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
    - strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.

    Returns:
    bool: True if the file exists, False otherwise.
    """
    if delimiter and delimiter == '\t' and not fileName.endswith('.tsv'):
        __teePrintOrNot(f'Warning: Filename {fileName} does not end with .tsv','warning',teeLogger=teeLogger)
    elif delimiter and delimiter == ',' and not fileName.endswith('.csv'):
        __teePrintOrNot(f'Warning: Filename {fileName} does not end with .csv','warning',teeLogger=teeLogger)
    elif delimiter and delimiter == '\0' and not fileName.endswith('.nsv'):
        __teePrintOrNot(f'Warning: Filename {fileName} does not end with .nsv','warning',teeLogger=teeLogger)
    elif delimiter and delimiter == '|' and not fileName.endswith('.psv'):
        __teePrintOrNot(f'Warning: Filename {fileName} does not end with .psv','warning',teeLogger=teeLogger)
    if not os.path.isfile(fileName):
        if createIfNotExist:
            try:
                with open(fileName, mode ='w',encoding=encoding)as file:
                    file.write(header+'\n')
                __teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
                return True
            except:
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

def readTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False,taskDic = None,encoding = 'utf8',strict = True,delimiter = '\t',defaults = []):
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

    Returns:
    - OrderedDict: The dictionary containing the data from the Tabular file.

    Raises:
    - Exception: If the file is not found or there is a data format error.

    """
    return readTabularFile(fileName,teeLogger = teeLogger,header = header,createIfNotExist = createIfNotExist,lastLineOnly = lastLineOnly,verifyHeader = verifyHeader,verbose = verbose,taskDic = taskDic,encoding = encoding,strict = strict,delimiter = delimiter,defaults=defaults)

def readTabularFile(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False,taskDic = None,encoding = 'utf8',strict = True,delimiter = ...,defaults = []):
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

    Returns:
    - OrderedDict: The dictionary containing the data from the Tabular file.

    Raises:
    - Exception: If the file is not found or there is a data format error.

    """
    if taskDic is None:
        taskDic = {}
    delimiter = get_delimiter(delimiter,file_name=fileName)
    header = _formatHeader(header,verbose = verbose,teeLogger = teeLogger, delimiter = delimiter)
    if not _verifyFileExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict,delimiter=delimiter):
        return taskDic
    with open(fileName, mode ='rb')as file:
        correctColumnNum = -1
        if header.rstrip() and verifyHeader:
                line = file.readline().decode(encoding=encoding)
                if _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
                    correctColumnNum = len(header.split(delimiter))
                    if verbose:
                        __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        if lastLineOnly:
            lineCache = read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=verbose, teeLogger=teeLogger, strict=strict, delimiter=delimiter, defaults=defaults)
            if lineCache:
                taskDic[lineCache[0]] = lineCache
            return lineCache
        for line in file:
            correctColumnNum, lineCache = _processLine(line.decode(encoding=encoding),taskDic,correctColumnNum,verbose = verbose,teeLogger = teeLogger,strict = strict,delimiter=delimiter,defaults = defaults)
    return taskDic

def appendTSV(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True, delimiter = '\t'):
    """
    Compatibility method, calls appendTabularFile.
    Append a line of data to a Tabular file.
    Parameters:
    - fileName (str): The path of the Tabular file.
    - lineToAppend (str or list): The line of data to append. If it is a string, it will be split by delimiter to form a list.
    - teeLogger (optional): A logger object for logging messages.
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
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
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
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
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
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
                        line[i] = str(line[i])
                    except Exception as e:
                        line[i] = str(e)
        if isinstance(linesToAppend,dict):
            if (not line or line[0] != key):
                line = [key]+line
        formatedLines.append(line)
    if not formatedLines:
        if verbose:
            __teePrintOrNot(f"No lines to append to {fileName}",teeLogger=teeLogger)
        return
    with open(fileName, mode ='r+b')as file:
        correctColumnNum = max([len(line) for line in formatedLines])
        if header.rstrip() and verifyHeader:
                line = file.readline().decode(encoding=encoding)
                if _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
                    correctColumnNum = len(header.split(delimiter))
                    if verbose:
                        __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        # truncate / fill the lines to the correct number of columns
        for i in range(len(formatedLines)):
            if len(formatedLines[i]) < correctColumnNum:
                formatedLines[i] += ['']*(correctColumnNum-len(formatedLines[i]))
            elif len(formatedLines[i]) > correctColumnNum:
                formatedLines[i] = formatedLines[i][:correctColumnNum]
        # check if the file ends in a newline
        file.seek(-1, os.SEEK_END)
        if file.read(1) != b'\n':
            file.write(b'\n')
        file.write(b'\n'.join([delimiter.join(line).encode(encoding=encoding) for line in formatedLines]) + b'\n')
        if verbose:
            __teePrintOrNot(f"Appended {len(formatedLines)} lines to {fileName}",teeLogger=teeLogger)

def clearTSV(fileName,teeLogger = None,header = '',verifyHeader = False,verbose = False,encoding = 'utf8',strict = False,delimiter = '\t'):
    """
    Compatibility method, calls clearTabularFile.
    Clear the contents of a Tabular file. Will create if not exist.
    Parameters:
    - fileName (str): The path of the Tabular file.
    - teeLogger (optional): A logger object for logging messages.
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
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
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
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
        with open(fileName, mode ='r+',encoding=encoding)as file:
            if header.rstrip() and verifyHeader:
                line = file.readline()
                if not _lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
                    __teePrintOrNot(f'Warning: Header mismatch in {fileName}. Keeping original header in file...','warning',teeLogger)
                file.truncate()
            else:
                file.write(header+'\n')
    if verbose:
        __teePrintOrNot(f"Cleared {fileName}",teeLogger=teeLogger)

def getFileUpdateTimeNs(fileName):
    # return 0 if the file does not exist
    if not os.path.isfile(fileName):
        return 0
    try:
        return os.stat(fileName).st_mtime_ns
    except:
        __teePrintOrNot(f"Failed to get file update time for {fileName}",'error')
        return get_time_ns()

def get_time_ns():
    try:
        return time.time_ns()
    except:
        # try to get the time in nanoseconds
        return int(time.time()*1e9)
    
# create a tsv class that functions like a ordered dictionary but will update the file when modified
class TSVZed(OrderedDict):
    def __teePrintOrNot(self,message,level = 'info'):
        try:
            if self.teeLogger:
                self.teeLogger.teelog(message,level)
            else:
                print(message,flush=True)
        except Exception:
            print(message,flush=True)

    def getResourseUsage(self,return_dict = False):
        return get_resource_usage(return_dict = return_dict)

    def __init__ (self,fileName,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,monitor_external_changes = True,verbose = False,encoding = 'utf8',delimiter = ...,defualts = None,strict = False):
        super().__init__()
        self.version = version
        self.strict = strict
        self.externalFileUpdateTime = getFileUpdateTimeNs(fileName)
        self.lastUpdateTime = self.externalFileUpdateTime
        self._fileName = fileName
        self.teeLogger = teeLogger
        self.delimiter = get_delimiter(delimiter,file_name=fileName)
        self.defaults = defualts if defualts else []
        self.header = _formatHeader(header,verbose = verbose,teeLogger = self.teeLogger,delimiter=self.delimiter)
        self.correctColumnNum = -1
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
            return
        if isinstance(defaults,str):
            defaults = defaults.split(self.delimiter)
        elif not isinstance(defaults,list):
            try:
                defaults = list(defaults)
            except:
                if self.verbose:
                    self.__teePrintOrNot('Invalid defaults, setting defaults to empty.','error')
                defaults = []
                return
        if not any(defaults):
            defaults = []
            return
        if defaults[0] != DEFAULTS_INDICATOR_KEY:
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
        readTabularFile(self._fileName, teeLogger = self.teeLogger, header = self.header, createIfNotExist = self.createIfNotExist, verifyHeader = self.verifyHeader, verbose = self.verbose, taskDic = self,encoding = self.encoding if self.encoding else None, strict = self.strict, delimiter = self.delimiter, defaults=self.defaults)
        if self.verbose:
            self.__teePrintOrNot(f"Loaded {len(self)} records from {self._fileName}")
        if self.header and self.verifyHeader:
            self.correctColumnNum = len(self.header.split(self.delimiter))
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
        value = [(str(segment).rstrip() if not isinstance(segment,str) else segment.rstrip()) if segment else '' for segment in value]
        # escape the delimiter and newline characters
        value = [segment.replace(self.delimiter,'<sep>').replace('\n','\\n') for segment in value]
        # the first field in value should be the key
        # add it if it is not there
        if not value or value[0] != key:
            value = [key]+value
        # verify the value has the correct number of columns
        if self.correctColumnNum != 1 and len(value) == 1:
            # this means we want to clear / delete the key
            self.__delitem__(key)
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
                self.appendQueue.append(self.delimiter.join(value))
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
        self.appendQueue.append(self.delimiter.join(value))
        self.lastUpdateTime = get_time_ns()
        # if not self.appendThread.is_alive():
        #     self.commitAppendToFile()
        # else:
        #     self.appendEvent.set()

    
    def __delitem__(self,key):
        key = str(key).rstrip()
        if key == DEFAULTS_INDICATOR_KEY:
            self.defaults = []
            if self.verbose:
                self.__teePrintOrNot(f"Defaults cleared")
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
            emptyLine = key+self.delimiter*(self.correctColumnNum-1)
        elif len(self[key]) > 1:
            self.correctColumnNum = len(self[key])
            emptyLine = key+self.delimiter*(self.correctColumnNum-1)
        else:
            emptyLine = key
        if self.verbose:
            self.__teePrintOrNot(f"Appending {emptyLine} to the appendQueue")
        self.appendQueue.append(emptyLine)
        return self

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
                file = self.get_file_obj('w')
                file.write(self.header+'\n')
                self.release_file_obj(file)
                if self.verbose:
                    self.__teePrintOrNot(f"Header {self.header} written to {self._fileName}")
                    self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
            else:
                file = self.get_file_obj('w')
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
        super().move_to_end(key, last)
        self.dirty = True
        if not self.rewrite_on_exit:
            self.rewrite_on_exit = True
            self.__teePrintOrNot(f"Warning: move_to_end had been called. Need to resync for changes to apply to disk.")
            self.__teePrintOrNot(f"rewrite_on_exit set to True")
        if self.verbose:
            self.__teePrintOrNot(f"Warning: Trying to move Key {key} moved to {'end' if last else 'beginning'} Need to resync for changes to apply to disk")
        self.lastUpdateTime = get_time_ns()
        return self
    
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
                    self.__teePrintOrNot(f"Memory only mode. Map to file skipped.")
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
        
    def oldMapToFile(self):
        try:
            if (not self.monitor_external_changes) and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
                self.__teePrintOrNot(f"Warning: Overwriting external changes in {self._fileName}",'warning')
            file = self.get_file_obj('w')
            if self.header:
                file.write(self.header+'\n')
            for key in self:
                file.write(self.delimiter.join(self[key])+'\n')
            self.release_file_obj(file)
            if self.verbose:
                self.__teePrintOrNot(f"{len(self)} records written to {self._fileName}")
                self.__teePrintOrNot(f"File {self._fileName} size: {os.path.getsize(self._fileName)}")
            self.dirty = False
            self.deSynced = False
        except Exception as e:
            self.release_file_obj(file)
            self.__teePrintOrNot(f"Failed to write at oldMapToFile() to {self._fileName}: {e}",'error')
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
            file = self.get_file_obj('r+b')
            overWrite = False
            if self.header:
                line = file.readline().decode(self.encoding)
                aftPos = file.tell()
                if not _lineContainHeader(self.header,line,verbose = self.verbose,teeLogger = self.teeLogger,strict = self.strict):
                    file.seek(0)
                    file.write(f'{self.header}\n'.encode(encoding=self.encoding))
                    # if the header is not the same length as the line, we need to overwrite the file
                    if aftPos != file.tell():
                        overWrite = True
                    if self.verbose:
                        self.__teePrintOrNot(f"Header {self.header} written to {self._fileName}")
            for value in self.values():
                if value[0].startswith('#'):
                    continue
                strToWrite = self.delimiter.join(value)
                if overWrite:
                    if self.verbose:
                        self.__teePrintOrNot(f"Overwriting {value} to {self._fileName}")
                    file.write(strToWrite.encode(encoding=self.encoding)+b'\n')
                    continue
                pos = file.tell()
                line = file.readline()
                aftPos = file.tell()
                if not line or pos == aftPos:
                    if self.verbose:
                        self.__teePrintOrNot(f"End of file reached. Appending {value} to {self._fileName}")
                    file.write(strToWrite.encode(encoding=self.encoding))
                    overWrite = True
                    continue
                strToWrite = strToWrite.encode(encoding=self.encoding).ljust(len(line)-1)+b'\n'
                if line != strToWrite:
                    if self.verbose:
                        self.__teePrintOrNot(f"Modifing {value} to {self._fileName}")
                    file.seek(pos)
                    # fill the string with space to write to the correct length
                    #file.write(strToWrite.rstrip('\n').ljust(len(line)-1)+'\n')
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
                    self.__teePrintOrNot(f"Memory only mode. Append queue cleared.") 
                return self
            try:
                if self.verbose:
                    self.__teePrintOrNot(f"Commiting {len(self.appendQueue)} records to {self._fileName}")
                    self.__teePrintOrNot(f"Before size of {self._fileName}: {os.path.getsize(self._fileName)}")
                file = self.get_file_obj('a')
                while self.appendQueue:
                    line = self.appendQueue.popleft()
                    file.write(line+'\n')
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
    
    def get_file_obj(self,modes = 'a'):
        self.writeLock.acquire()
        try:
            if 'b' not in modes:
                if not self.encoding:
                    self.encoding = 'utf8'
                file = open(self._fileName, mode=modes, encoding=self.encoding)
            else:
                file = open(self._fileName, mode=modes)
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
                    except:
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


def __main__():
    import argparse
    parser = argparse.ArgumentParser(description='TSVZed: A TSV / CSV / NSV file manager')
    parser.add_argument('filename', type=str, help='The file to read')
    parser.add_argument('operation', type=str,nargs='?', choices=['read','append','delete','clear'], help='The operation to perform. Default: read', default='read')
    parser.add_argument('line', type=str, nargs='*', help='The line to append to the Tabular file. it follows as : {key} {value1} {value2} ... if a key without value be inserted, the value will get deleted.')
    parser.add_argument('-d', '--delimiter', type=str, help='The delimiter of the Tabular file. Default: Infer from last part of filename, or tab if cannot determine. Note: accept unicode escaped char, raw char, or string "comma,tab,null" will refer to their characters. ', default=...)
    parser.add_argument('-c', '--header', type=str, help='Perform checks with this header of the Tabular file. seperate using --delimiter.')
    parser.add_argument('--defaults', type=str, help='Default values to fill in the missing columns. seperate using --delimiter. Ex. if -d = comma, --defaults="key,value1,value2..." Note: Please specify the key. But it will not be used as a key need to be unique in data.')
    strictMode = parser.add_mutually_exclusive_group()
    strictMode.add_argument('-s', '--strict', dest = 'strict',action='store_true', help='Strict mode. Do not parse values that seems malformed, check for column numbers / headers')
    strictMode.add_argument('-f', '--force', dest = 'strict',action='store_false', help='Force the operation. Ignore checks for column numbers / headers')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output')
    parser.add_argument('-V', '--version', action='version', version=f'%(prog)s {version} by {author}')
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
    else:
        print("Invalid operation")    
if __name__ == '__main__':
    __main__()


    