#!/usr/bin/env python3
import os
from collections import OrderedDict , deque
import time
import atexit
import threading

if os.name == 'nt':
    import msvcrt
elif os.name == 'posix':
    import fcntl

version = '2.57'

def processLine(line,taskDic,correctColumnNum,verbose = False,teeLogger = None,strict = True):
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


def readTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False,taskDic = None,encoding = 'utf8',strict = True):
    if taskDic is None:
        taskDic = OrderedDict()

    header = header.strip() if type(header) == str else '\t'.join(header)
    if not header.endswith('\n'):
        header += '\n'
    if not os.path.isfile(fileName):
        if createIfNotExist:
            with open(fileName, mode ='w',encoding=encoding)as file:
                file.write(header)
            __teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
            verifyHeader = True
        else:
            __teePrintOrNot('File not found','error',teeLogger=teeLogger)
            raise Exception("File not found")
    with open(fileName, mode ='rb')as file:
        if header.strip():
            if verifyHeader:
                line = file.readline().decode().strip()
                if verbose:
                    __teePrintOrNot(f"Header: {header.strip()}",teeLogger=teeLogger)
                    __teePrintOrNot(f"First line: {line}",teeLogger=teeLogger)
                #assert line.lower().replace(' ','').startswith(header.strip().lower().replace(' ','')), "Data format error!"
                if not line.lower().replace(' ','').startswith(header.strip().lower().replace(' ','')):
                    __teePrintOrNot(f"Header mismatch: \n{line} \n!= \n{header.strip()}",teeLogger=teeLogger)
                    raise Exception("Data format error! Header mismatch")
            correctColumnNum = len(header.strip().split('\t'))
            if verbose:
                __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        else:
            correctColumnNum = -1
        if lastLineOnly:
            lineCache = read_last_valid_line(fileName, taskDic, correctColumnNum, verbose=verbose, teeLogger=teeLogger, strict=strict)
            if lineCache:
                taskDic[lineCache[0]] = lineCache
            return lineCache
        for line in file:
            correctColumnNum, lineCache = processLine(line,taskDic,correctColumnNum,verbose = verbose,teeLogger = teeLogger,strict = strict)
    return taskDic



def __teePrintOrNot(message,level = 'info',teeLogger = None):
    try:
        if teeLogger:
            teeLogger.teelog(message,level)
        else:
            print(message)
    except Exception as e:
        print(message)


def appendTSV(fileName,lineToAppend,teeLogger = None,header = '',createIfNotExist = False,verifyHeader = True,verbose = False,encoding = 'utf8'):
    if not header.endswith('\n'):
        header += '\n'
    if not os.path.isfile(fileName):
        if createIfNotExist:
            with open(fileName, mode ='w',encoding=encoding)as file:
                file.write(header)
            __teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
            verifyHeader = True
        else:
            __teePrintOrNot('File not found','error',teeLogger=teeLogger)
            raise Exception("File not found")
    
    if type(lineToAppend) == str:
        lineToAppend = lineToAppend.strip().split('\t')
    
    with open(fileName, mode ='r+b')as file:
        if header.strip():
            if verifyHeader:
                line = file.readline().decode().strip()
                if verbose:
                    __teePrintOrNot(f"Header: {header.strip()}",teeLogger=teeLogger)
                    __teePrintOrNot(f"First line: {line}",teeLogger=teeLogger)
                #assert line.lower().replace(' ','').startswith(header.strip().lower().replace(' ','')), "Data format error!"
                if not line.lower().replace(' ','').startswith(header.strip().lower().replace(' ','')):
                    __teePrintOrNot(f"Header mismatch: \n{line} \n!= \n{header.strip()}",teeLogger=teeLogger)
                    raise Exception("Data format error! Header mismatch")
            correctColumnNum = len(header.strip().split('\t'))
            if verbose:
                __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        else:
            correctColumnNum = len(lineToAppend)
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



# create a tsv class that functions like a ordered dictionary but will update the file when modified
class TSVZed(OrderedDict):
    def __teePrintOrNot(self,message,level = 'info'):
        try:
            if self.teeLogger:
                self.teeLogger.teelog(message,level)
            else:
                print(message)
        except Exception as e:
            print(message)

    def __init__ (self,fileName,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,monitor_external_changes = True,verbose = False,encoding = None):
        super().__init__()
        self.version = version
        self._fileName = fileName
        self.teeLogger = teeLogger
        self.header = header.strip() if type(header) == str else '\t'.join(header)
        self.correctColumnNum = -1
        self.createIfNotExist = createIfNotExist
        self.verifyHeader = verifyHeader
        self.rewrite_on_load = rewrite_on_load
        self.rewrite_on_exit = rewrite_on_exit
        self.rewrite_interval = rewrite_interval
        self.monitor_external_changes = monitor_external_changes
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
        if not self.dirty and not force:
            return False
        if not self.deSynced and not force:
            if self.rewrite_interval == 0 or time.time() - os.path.getmtime(self._fileName) < self.rewrite_interval:
                return False
        try:
            if self.verbose:
                self.__teePrintOrNot(f"Rewriting {self._fileName}")
            if reloadInternalFromFile is None:
                reloadInternalFromFile = self.monitor_external_changes
            if reloadInternalFromFile:
                # this will be needed if more than 1 process is accessing the file
                self.commitAppendToFile()
                self.reload()
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
        
    def mapToFile(self):
        try:
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
            self.__teePrintOrNot(f"Failed to write at dumpToFile() to {self._fileName}: {e}",'error')
            import traceback
            self.__teePrintOrNot(traceback.format_exc(),'error')
            self.deSynced = True
        return self

    def _appendWorker(self):
        while not self.shutdownEvent.is_set():
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
            self.writeLock.release()  # Release the thread lock in case of an error
            raise e  # Re-raise the exception to handle it outside or notify the user
        return file

    def release_file_obj(self,file):
        try:
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
            raise e  # Re-raise the exception for external handling
        finally:
            self.writeLock.release()  # Ensure the thread lock is always released
