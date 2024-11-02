#!/usr/bin/env python3
import os , sys
from collections import OrderedDict , deque
import time
import atexit
import threading

if os.name == 'nt':
    import msvcrt
elif os.name == 'posix':
    import fcntl

version = '2.65'
author = 'pan@zopyr.us'


def pretty_format_table(data):
	if not data:
		return
	if type(data) == str:
		data = data.strip('\n').split('\n')
	elif type(data) != list:
		data = list(data)
	num_cols = len(data[0])
	col_widths = [0] * num_cols
	# Calculate the maximum width of each column
	for c in range(num_cols):
		col_items = [str(row[c]) for row in data]
		col_widths[c] = max(len(item) for item in col_items)
	# Build the row format string
	row_format = ' | '.join('{{:<{}}}'.format(width) for width in col_widths)
	# Print the header
	header = data[0]
	outTable = []
	outTable.append(row_format.format(*header))
	outTable.append('-+-'.join('-' * width for width in col_widths))
	for row in data[1:]:
		outTable.append(row_format.format(*row))
	return '\n'.join(outTable) + '\n'

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
            teeLogger.teelog(message,level)
        else:
            print(message,flush=True)
    except Exception as e:
        print(message,flush=True)

def processLine(line,taskDic,correctColumnNum,verbose = False,teeLogger = None,strict = True):
    """
    Process a line of text and update the task dictionary.

    Parameters:
    line (str): The line of text to process.
    taskDic (dict): The dictionary to update with the processed line.
    correctColumnNum (int): The expected number of columns in the line.
    verbose (bool, optional): Whether to print verbose output. Defaults to False.
    teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
    strict (bool, optional): Whether to strictly enforce the correct number of columns. Defaults to True.

    Returns:
    tuple: A tuple containing the updated correctColumnNum and the processed lineCache.

    """
    line = line.decode().strip(' ').strip('\x00')
    # we throw away the lines that start with '#'
    if not line :
        if verbose:
            __teePrintOrNot(f"Ignoring empty line: {line}",teeLogger=teeLogger)
        return correctColumnNum , []
    if line.startswith('#'):
        if verbose:
            __teePrintOrNot(f"Ignoring comment line: {line}",teeLogger=teeLogger)
        return correctColumnNum , []
    # we only interested in the lines that have the correct number of columns
    lineCache = [segment.strip() for segment in line.split('\t')]
    if not lineCache:
        return correctColumnNum , []
    if correctColumnNum == -1:
        if verbose:
            __teePrintOrNot(f"detected correctColumnNum: {len(lineCache)}",teeLogger=teeLogger)
        correctColumnNum = len(lineCache)
    if not lineCache[0]:
        if verbose:
            __teePrintOrNot(f"Ignoring line with empty key: {line}",teeLogger=teeLogger)
        return correctColumnNum , []
    if len(lineCache) == 1 or not any(lineCache[1:]):
        if correctColumnNum == 1: taskDic[lineCache[0]] = lineCache
        else:
            if verbose:
                __teePrintOrNot(f"Key {lineCache[0]} found with empty value, deleting such key's representaion",teeLogger=teeLogger)
            if lineCache[0] in taskDic:
                del taskDic[lineCache[0]]
        return correctColumnNum , []
    elif len(lineCache) == correctColumnNum:
        taskDic[lineCache[0]] = lineCache
        if verbose:
            __teePrintOrNot(f"Key {lineCache[0]} added",teeLogger=teeLogger)
    else:
        if strict:
            if verbose:
                __teePrintOrNot(f"Ignoring line with {len(lineCache)} columns: {line}",teeLogger=teeLogger)
            return correctColumnNum , []
        else:
            # fill / cut the line with empty entries til the correct number of columns
            if len(lineCache) < correctColumnNum:
                lineCache += ['']*(correctColumnNum-len(lineCache))
            elif len(lineCache) > correctColumnNum:
                lineCache = lineCache[:correctColumnNum]
            taskDic[lineCache[0]] = lineCache
            if verbose:
                __teePrintOrNot(f"Key {lineCache[0]} added after correction",teeLogger=teeLogger)
    return correctColumnNum, lineCache

def read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=False, teeLogger=None, strict=False):
    """
    Reads the last valid line from a file.

    Args:
        fileName (str): The name of the file to read.
        taskDic (dict): A dictionary to pass to processLine function.
        correctColumnNum (int): A column number to pass to processLine function.
        verbose (bool, optional): Whether to print verbose output. Defaults to False.
        teeLogger (optional): Logger to use for tee print. Defaults to None.
        strict (bool, optional): Whether to enforce strict processing. Defaults to False.

    Returns:
        list: The last valid line data processed by processLine, or an empty list if none found.
    """
    chunk_size = 1024  # Read in chunks of 1024 bytes
    last_valid_line = []
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
                    correctColumnNum, lineCache = processLine(
                        lines[i],
                        taskDic,
                        correctColumnNum,
                        verbose=verbose,
                        teeLogger=teeLogger,
                        strict=strict
                    )
                    # If the line is valid, return it
                    if lineCache and any(lineCache):
                        return lineCache
            
            # Keep the last (possibly incomplete) line in buffer for the next read
            buffer = lines[0]

    # Return empty list if no valid line found
    return last_valid_line

def formatHeader(header,verbose = False,teeLogger = None):
    """
    Format the header string.

    Parameters:
    - header (str or list): The header string or list to format.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.
    - teeLogger (object, optional): The tee logger object for printing output. Defaults to None.

    Returns:
        str: The formatted header string.
    """
    if type(header) != str:
        try:
            header = '\t'.join(header)
        except:
            if verbose:
                __teePrintOrNot('Invalid header, setting header to empty.','error',teeLogger=teeLogger)
            header = ''
    header = header.strip()
    # if header:
    #     if not header.endswith('\n'):
    #         header += '\n'
    # else:
    #     header = ''
    return header

def lineContainHeader(header,line,verbose = False,teeLogger = None,strict = False):
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
    if verbose:
        __teePrintOrNot(f"Header: {header.strip()}",teeLogger=teeLogger)
        __teePrintOrNot(f"First line: {line}",teeLogger=teeLogger)
    if not line.lower().replace(' ','').startswith(header.strip().lower().replace(' ','')):
        __teePrintOrNot(f"Header mismatch: \n{line} \n!= \n{header.strip()}",teeLogger=teeLogger)
        if strict:
            raise Exception("Data format error! Header mismatch")
        return False
    return True

def verifyTSVExistence(fileName,createIfNotExist = True,teeLogger = None,header = '',encoding = 'utf8',strict = True):
    """
    Verify the existence of a TSV file.

    Parameters:
    - fileName (str): The path of the TSV file.
    - createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to True.
    - teeLogger (object, optional): The tee logger object for printing output. Defaults to None.
    - header (str, optional): The header line to verify against. Defaults to ''.
    - encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
    - strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.

    Returns:
    bool: True if the file exists, False otherwise.
    """
    if not fileName.endswith('.tsv'):
        __teePrintOrNot(f'Warning: Filename {fileName} does not end with .tsv','warning',teeLogger=teeLogger)
    if not os.path.isfile(fileName):
        if createIfNotExist:
            with open(fileName, mode ='w',encoding=encoding)as file:
                file.write(header+'\n')
            __teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
            return True
        elif strict:
            __teePrintOrNot('File not found','error',teeLogger=teeLogger)
            raise Exception("File not found")
        else:
            return False
    return True

def readTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False,taskDic = None,encoding = 'utf8',strict = True):
    """
    Read a TSV (Tab-Separated Values) file and return the data as a dictionary.

    Parameters:
    - fileName (str): The path to the TSV file.
    - teeLogger (Logger, optional): The logger object to log messages. Defaults to None.
    - header (str or list, optional): The header of the TSV file. If a string, it should be a tab-separated list of column names. If a list, it should contain the column names. Defaults to ''.
    - createIfNotExist (bool, optional): Whether to create the file if it doesn't exist. Defaults to False.
    - lastLineOnly (bool, optional): Whether to read only the last valid line of the file. Defaults to False.
    - verifyHeader (bool, optional): Whether to verify the header of the file. Defaults to True.
    - verbose (bool, optional): Whether to print verbose output. Defaults to False.
    - taskDic (OrderedDict, optional): The dictionary to store the data. Defaults to an empty OrderedDict.
    - encoding (str, optional): The encoding of the file. Defaults to 'utf8'.
    - strict (bool, optional): Whether to raise an exception if there is a data format error. Defaults to True.

    Returns:
    - OrderedDict: The dictionary containing the data from the TSV file.

    Raises:
    - Exception: If the file is not found or there is a data format error.

    """
    if taskDic is None:
        taskDic = {}
    header = formatHeader(header,verbose = verbose,teeLogger = teeLogger)
    if not verifyTSVExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict):
        return taskDic
    with open(fileName, mode ='rb')as file:
        correctColumnNum = -1
        if header.strip():
            if verifyHeader:
                line = file.readline().decode().strip()
                if lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
                    correctColumnNum = len(header.strip().split('\t'))
                    if verbose:
                        __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        if lastLineOnly:
            lineCache = read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=verbose, teeLogger=teeLogger, strict=strict)
            if lineCache:
                taskDic[lineCache[0]] = lineCache
            return lineCache
        for line in file:
            correctColumnNum, lineCache = processLine(line,taskDic,correctColumnNum,verbose = verbose,teeLogger = teeLogger,strict = strict)
    return taskDic

def appendTSV(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8', strict = True):
    """
    Append a line of data to a TSV file.
    Parameters:
    - fileName (str): The path of the TSV file.
    - lineToAppend (str or list): The line of data to append. If it is a string, it will be split by tabs ('\t') to form a list.
    - teeLogger (optional): A logger object for logging messages.
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
    - createIfNotExist (bool, optional): If True, the file will be created if it does not exist. If False and the file does not exist, an exception will be raised.
    - verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
    - verbose (bool, optional): If True, additional information will be printed during the execution.
    - encoding (str, optional): The encoding of the file.
    - strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
    Raises:
    - Exception: If the file does not exist and createIfNotExist is False.
    - Exception: If the existing header does not match the provided header.
    """
    header = formatHeader(header,verbose = verbose,teeLogger = teeLogger)
    if not verifyTSVExistence(fileName,createIfNotExist = createIfNotExist,teeLogger = teeLogger,header = header,encoding = encoding,strict = strict):
        return
    if type(lineToAppend) == str:
        lineToAppend = lineToAppend.strip().split('\t')
    
    with open(fileName, mode ='r+b')as file:
        correctColumnNum = len(lineToAppend)
        if header.strip():
            if verifyHeader:
                line = file.readline().decode().strip()
                if lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
                    correctColumnNum = len(header.strip().split('\t'))
                    if verbose:
                        __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        # truncate / fill the lineToAppend to the correct number of columns
        if len(lineToAppend) < correctColumnNum:
            lineToAppend += ['']*(correctColumnNum-len(lineToAppend))
        elif len(lineToAppend) > correctColumnNum:
            lineToAppend = lineToAppend[:correctColumnNum]
        # check if the file ends in a newline
        file.seek(-1, os.SEEK_END)
        if file.read(1) != b'\n':
            file.write(b'\n')
        file.write('\t'.join(lineToAppend).encode() + b'\n')
        if verbose:
            __teePrintOrNot(f"Appended {lineToAppend} to {fileName}",teeLogger=teeLogger)

def clearTSV(fileName,teeLogger = None,header = '',verifyHeader = False,verbose = False,encoding = 'utf8',strict = False):
    """
    Clear the contents of a TSV file. Will create if not exist.
    Parameters:
    - fileName (str): The path of the TSV file.
    - teeLogger (optional): A logger object for logging messages.
    - header (str, optional): The header line to verify against. If provided, the function will check if the existing header matches the provided header.
    - verifyHeader (bool, optional): If True, the function will verify if the existing header matches the provided header. If False, the header will not be verified.
    - verbose (bool, optional): If True, additional information will be printed during the execution.
    - encoding (str, optional): The encoding of the file.
    - strict (bool, optional): If True, the function will raise an exception if there is a data format error. If False, the function will ignore the error and continue.
    """
    header = formatHeader(header,verbose = verbose,teeLogger = teeLogger)
    if not verifyTSVExistence(fileName,createIfNotExist = True,teeLogger = teeLogger,header = header,encoding = encoding,strict = False):
        raise Exception("Something catastrophic happened! File still not found after creation")
    else:
        with open(fileName, mode ='r+',encoding=encoding)as file:
            if header.strip() and verifyHeader:
                line = file.readline().strip()
                if not lineContainHeader(header,line,verbose = verbose,teeLogger = teeLogger,strict = strict):
                    __teePrintOrNot(f'Warning: Header mismatch in {fileName}. Keeping original header in file...','warning',teeLogger)
                file.truncate()
            else:
                file.write(header+'\n')
    if verbose:
        __teePrintOrNot(f"Cleared {fileName}",teeLogger=teeLogger)

def getFileUpdateTimeNs(fileName):
    try:
        return os.stat(fileName).st_mtime_ns
    except:
        __teePrintOrNot(f"Failed to get file update time for {fileName}",'error')
        return time.time_ns()

# create a tsv class that functions like a ordered dictionary but will update the file when modified
class TSVZed(OrderedDict):
    def __teePrintOrNot(self,message,level = 'info'):
        try:
            if self.teeLogger:
                self.teeLogger.teelog(message,level)
            else:
                print(message,flush=True)
        except Exception as e:
            print(message,flush=True)

    def __init__ (self,fileName,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,monitor_external_changes = True,verbose = False,encoding = None):
        super().__init__()
        self.version = version
        self.externalFileUpdateTime = getFileUpdateTimeNs(fileName)
        self.lastUpdateTime = self.externalFileUpdateTime
        self._fileName = fileName
        self.teeLogger = teeLogger
        self.header = formatHeader(header,verbose = verbose,teeLogger = self.teeLogger)
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
        readTSV(self._fileName, teeLogger = self.teeLogger, header = self.header, createIfNotExist = self.createIfNotExist, verifyHeader = self.verifyHeader, verbose = self.verbose, taskDic = self,encoding = self.encoding if self.encoding else None)
        if self.verbose:
            self.__teePrintOrNot(f"Loaded {len(self)} records from {self._fileName}")
        self.correctColumnNum = len(self.header.split('\t')) if (self.header and self.verifyHeader) else (len(self[next(iter(self))]) if self else -1)
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
        key = str(key).strip()
        if not key:
            self.__teePrintOrNot('Key cannot be empty','error')
            return
        if type(value) == str:
            value = value.strip().split('\t')
        # sanitize the value
        value = [(str(segment).strip() if type(segment) != str else segment.strip()) if segment else '' for segment in value]
        #value = list(map(lambda segment: str(segment).strip(), value))
        # the first field in value should be the key
        # add it if it is not there
        if not value or value[0] != key:
            value = [key]+value
        # verify the value has the correct number of columns
        if self.correctColumnNum != 1 and len(value) == 1:
            # this means we want to clear / deelte the key
            self.__delitem__(key)
        elif self.correctColumnNum > 0:
            assert len(value) == self.correctColumnNum, f"Data format error! Expected {self.correctColumnNum} columns, but got {len(value) } columns"
        else:
            self.correctColumnNum = len(value)
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
        if self.verbose:
            self.__teePrintOrNot(f"Key {key} updated")
        if self.memoryOnly:
            return
        if self.verbose:
            self.__teePrintOrNot(f"Appending {key} to the appendQueue")
        self.appendQueue.append('\t'.join(value))
        self.lastUpdateTime = time.time_ns()
        # if not self.appendThread.is_alive():
        #     self.commitAppendToFile()
        # else:
        #     self.appendEvent.set()

    
    def __delitem__(self,key):
        key = str(key).strip()
        # delete the key from the dictionary and update the file
        if key not in self:
            if self.verbose:
                self.__teePrintOrNot(f"Key {key} not found")
            return
        super().__delitem__(key)
        if self.memoryOnly:
            return
        self.__appendEmptyLine(key)
        self.lastUpdateTime = time.time_ns()
        
    def __appendEmptyLine(self,key):
        self.dirty = True
        if self.correctColumnNum > 0:
            emptyLine = key+'\t'*(self.correctColumnNum-1)
        elif len(self[key]) > 1:
            self.correctColumnNum = len(self[key])
            emptyLine = key+'\t'*(self.correctColumnNum-1)
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
    
    def __exit__(self,exc_type,exc_value,traceback):
        self.stopAppendThread()
        return self
    
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
    
    def close(self):
        self.stopAppendThread()
        return self
    
    def __str__(self):
        return f"TSVZed({self._fileName},{dict(self)})"

    def __del__(self):
        self.stopAppendThread()
        return self

    def popitem(self, last=True):
        key, value = super().popitem(last)
        if not self.memoryOnly:
            self.__appendEmptyLine(key)
        self.lastUpdateTime = time.time_ns()
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
        self.lastUpdateTime = time.time_ns()
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
        self.lastUpdateTime = time.time_ns()
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
                file.write('\t'.join(self[key])+'\n')
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
        try:
            if (not self.monitor_external_changes) and self.externalFileUpdateTime < getFileUpdateTimeNs(self._fileName):
                self.__teePrintOrNot(f"Warning: Overwriting external changes in {self._fileName}",'warning')
            file = self.get_file_obj('r+')
            overWrite = False
            line = file.readline()
            aftPos = file.tell()
            if self.header and not lineContainHeader(self.header,line,verbose = self.verbose,teeLogger = self.teeLogger,strict = False):
                file.seek(0)
                file.write(self.header+'\n')
                # if the header is not the same length as the line, we need to overwrite the file
                if aftPos != file.tell():
                    overWrite = True
                if self.verbose:
                    self.__teePrintOrNot(f"Header {self.header} written to {self._fileName}")
            for value in self.values():
                strToWrite = '\t'.join(value)+'\n'
                if overWrite:
                    if self.verbose:
                        self.__teePrintOrNot(f"Overwriting {value} to {self._fileName}")
                    file.write(strToWrite)
                    continue
                pos = file.tell()
                line = file.readline()
                aftPos = file.tell()
                if not line or pos == aftPos:
                    if self.verbose:
                        self.__teePrintOrNot(f"End of file reached. Appending {value} to {self._fileName}")
                    file.write(strToWrite)
                    overWrite = True
                    continue
                if line != strToWrite:
                    if self.verbose:
                        self.__teePrintOrNot(f"Overwriting {value} to {self._fileName}")
                    file.seek(pos)
                    # fill the string with space to write to the correct length
                    file.write(strToWrite.rstrip('\n').ljust(len(line)-1)+'\n')
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
        return self
    
    def checkExternalChanges(self):
        if self.deSynced:
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
            if not self.encoding:
                self.encoding = 'utf8'
            file = open(self._fileName, mode=modes, encoding=self.encoding)
            # Lock the file after opening
            if os.name == 'posix':
                fcntl.lockf(file, fcntl.LOCK_EX)
            elif os.name == 'nt':
                # For Windows, locking the entire file, avoiding locking an empty file
                lock_length = max(1, os.path.getsize(self._fileName))
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
            if os.name == 'posix':
                fcntl.lockf(file, fcntl.LOCK_UN)
            elif os.name == 'nt':
                # Unlocking the entire file; for Windows, ensure not unlocking an empty file
                unlock_length = max(1, os.path.getsize(file.name))
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, unlock_length)
            file.close()  # Ensure file is closed after unlocking
            if self.verbose:
                self.__teePrintOrNot(f"File {file.name} unlocked / released")
        except Exception as e:
            self.__teePrintOrNot(f"Failed to release file {file.name}: {e}",'error')
        finally:
            try:
                self.writeLock.release()  # Ensure the thread lock is always released
            except Exception as e:
                self.__teePrintOrNot(f"Failed to release writeLock for {file.name}: {e}",'error')
        self.externalFileUpdateTime = getFileUpdateTimeNs(self._fileName)


def __main__():
    import argparse
    parser = argparse.ArgumentParser(description='TSVZed: A TSV file manager')
    parser.add_argument('filename', type=str, help='The TSV file to read')
    parser.add_argument('operation', type=str,nargs='?', choices=['read','append','delete','clear'], help='The operation to perform. Default: read', default='read')
    parser.add_argument('line', type=str, nargs='*', help='The line to append to the TSV file. it follows as : {key} {value1} {value2} ... if a key without value be inserted, the value will get deleted.')
    parser.add_argument('-c', '--header', type=str, help='Perform checks with this header of the TSV file. seperate using \\t')
    parser.add_argument('-f', '--force', action='store_true', help='Force the operation. Ignore checks for column numbers / headers')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print verbose output')
    parser.add_argument('-V', '--version', action='version', version=f'%(prog)s {version} by {author}')
    args = parser.parse_args()

    header = args.header.replace('\\t','\t') if args.header else ''

    if args.operation == 'read':
        # check if the file exist
        if not os.path.isfile(args.filename):
            print(f"File not found: {args.filename}")
            return
        # read the file
        data = readTSV(args.filename, verifyHeader = False, verbose=args.verbose,strict= not args.force)
        print(pretty_format_table(data.values()))
    elif args.operation == 'append':
        appendTSV(args.filename, args.line,createIfNotExist = True, header=header, verbose=args.verbose, strict= not args.force)
    elif args.operation == 'delete':
        appendTSV(args.filename, args.line[:1],createIfNotExist = True, header=header, verbose=args.verbose, strict= not args.force)
    elif args.operation == 'clear':
        clearTSV(args.filename, header=header, verbose=args.verbose, verifyHeader=not args.force)
    else:
        print("Invalid operation")
        return
    
if __name__ == '__main__':
    __main__()


    