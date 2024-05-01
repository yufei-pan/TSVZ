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

version = '2.37'
def readTSV(fileName,teeLogger = None,header = '',createIfNotExist = False, lastLineOnly = False,verifyHeader = True,verbose = False):
    if not header.endswith('\n'):
        header += '\n'
    if not os.path.isfile(fileName):
        if createIfNotExist:
            with open(fileName, mode ='w',encoding='utf8')as file:
                file.write(header)
            __teePrintOrNot('Created '+fileName,teeLogger=teeLogger)
        else:
            __teePrintOrNot('File not found','error',teeLogger=teeLogger)
            raise Exception("File not found")
    
    taskDic = OrderedDict()
    with open(fileName, mode ='rb')as file:
        if header.strip():
            if verifyHeader:
                line = file.readline().decode().strip()
                if verbose:
                    __teePrintOrNot(f"Header: {header.strip()}",teeLogger=teeLogger)
                    __teePrintOrNot(f"First line: {line}",teeLogger=teeLogger)
                assert line.lower().replace(' ','').startswith(header.strip().lower().replace(' ','')), "Data format error!"
            correctColumnNum = len(header.strip().split('\t'))
            if verbose:
                __teePrintOrNot(f"correctColumnNum: {correctColumnNum}",teeLogger=teeLogger)
        else:
            correctColumnNum = -1
        if lastLineOnly:
            if verbose:
                __teePrintOrNot(f"Reading last line only from {fileName}",teeLogger=teeLogger)
            file.seek(-2, os.SEEK_END)
            line = []
            while not line :
                while file.read(1) != b'\n':
                    # if we are at the start of the file, we stop
                    if file.tell() == 1:
                        break
                    file.seek(-2, os.SEEK_CUR)
                line = file.readline().decode().strip()
                line = line.strip().strip('\x00')
                line = [segment.strip() for segment in line.strip().split('\t')] if line else []
                if correctColumnNum != -1 and len(line) != correctColumnNum:
                    if verbose:
                        __teePrintOrNot(f"Ignoring line with {len(line)} columns: {line}",teeLogger=teeLogger)
                    line = []
            return line
        for line in file:
            line = line.decode().strip().strip('\x00')
            # we throw away the lines that start with '#'
            if not line or line.startswith('#'):
                continue
            # we only interested in the lines that have the correct number of columns
            lineCache = [segment.strip() for segment in line.strip().split('\t')]
            if correctColumnNum == -1:
                if verbose:
                    __teePrintOrNot(f"detected correctColumnNum: {len(lineCache)}",teeLogger=teeLogger)
                correctColumnNum = len(lineCache)
            if len(lineCache) == correctColumnNum:
                taskDic[lineCache[0]] = lineCache
                if verbose:
                    __teePrintOrNot(f"Key {lineCache[0]} added",teeLogger=teeLogger)
            elif len(lineCache) == 1:
                if verbose:
                    __teePrintOrNot(f"Key {lineCache[0]} found with empty value, deleting such key's representaion",teeLogger=teeLogger)
                if lineCache[0] in taskDic:
                    del taskDic[lineCache[0]]
            else:
                if verbose:
                    __teePrintOrNot(f"Ignoring line with {len(lineCache)} columns: {line}",teeLogger=teeLogger)
    return taskDic



def __teePrintOrNot(message,level = 'info',teeLogger = None):
    if teeLogger:
        teeLogger.teelog(message,level)
    else:
        print(message)

# create a tsv class that functions like a ordered dictionary but will update the file when modified
class TSVZed(OrderedDict):
    def __teePrintOrNot(self,message,level = 'info'):
        if self.teeLogger:
            self.teeLogger.teelog(message,level)
        else:
            print(message)

    def __init__ (self,fileName,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,verbose = False):
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
        self.verbose = verbose
        if append_check_delay < 0:
            append_check_delay = 0.00001
            self.__teePrintOrNot('append_check_delay cannot be less than 0, setting it to 0.00001','error')
        self.append_check_delay = append_check_delay
        self.appendQueue = deque()
        self.dirty = False
        self.deSynced = False
        self.memoryOnly = False
        self.writeLock = threading.Lock()
        self.shutdownEvent = threading.Event()
        #self.appendEvent = threading.Event()
        self.appendThread  = threading.Thread(target=self._appendWorker,daemon=True)
        self.appendThread.start()
        self.load()
        atexit.register(self.stopAppendThread)

    def load(self):
        # Load or refresh data from the TSV file
        mo = self.memoryOnly
        if not mo:
            self.memoryOnly = True
        if self.verbose:
            self.__teePrintOrNot(f"Loading {self._fileName}")
        loadedData = readTSV(self._fileName, self.teeLogger, self.header, self.createIfNotExist, False, self.verifyHeader, self.verbose)
        if self.verbose:
            self.__teePrintOrNot(f"Loaded {len(loadedData)} records from {self._fileName}")
        self.correctColumnNum = len(self.header.split('\t')) if (self.header and self.verifyHeader) else (len(loadedData[next(iter(loadedData))]) if loadedData else -1)
        if self.verbose:
            self.__teePrintOrNot(f"correctColumnNum: {self.correctColumnNum}")
        super().clear()
        super().update(loadedData)
        if self.verbose:
            self.__teePrintOrNot(f"TSVZed({self._fileName}) loaded")
        if not mo:
            self.memoryOnly = False
        if self.rewrite_on_load:
            self.sync(rewrite = True)

    def __setitem__(self,key,value):
        key = str(key).strip()
        if not key:
            self.__teePrintOrNot('Key cannot be empty','error')
            return
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

    def clear(self):
        # clear the dictionary and update the file
        super().clear()
        if self.verbose:
            self.__teePrintOrNot(f"Clearing {self._fileName}")
        if self.memoryOnly:
            return
        self.clear_file()

    def clear_file(self):
        try:
            if self.header:
                file = self.get_file_obj('w')
                file.write(self.header+'\n')
                if self.verbose:
                    self.__teePrintOrNot(f"Header {self.header} written to {self._fileName}")
                self.release_file_obj(file)

            else:
                file = self.get_file_obj('w')
                if self.verbose:
                    self.__teePrintOrNot(f"File {self._fileName} cleared empty")
                self.release_file_obj(file)
            self.dirty = False
            self.deSynced = False
        except Exception as e:
            self.__teePrintOrNot(f"Failed to write to {self._fileName}: {e}",'error')
            self.deSynced = True

    def __enter__(self):
        return self
    
    def __exit__(self,exc_type,exc_value,traceback):
        self.stopAppendThread()
        return False
    
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
    
    def __str__(self):
        return f"TSVZed({self._fileName},{dict(self)})"

    def __del__(self):
        self.stopAppendThread()

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

    @classmethod
    def fromkeys(cls, iterable, value=None,fileName = None,teeLogger = None,header = '',createIfNotExist = True,verifyHeader = True,rewrite_on_load = True,rewrite_on_exit = False,rewrite_interval = 0, append_check_delay = 0.01,verbose = False):
        '''Create a new ordered dictionary with keys from iterable and values set to value.
        '''
        self = cls(fileName,teeLogger,header,createIfNotExist,verifyHeader,rewrite_on_load,rewrite_on_exit,rewrite_interval,append_check_delay,verbose)
        for key in iterable:
            self[key] = value
        return self


    def sync(self,rewrite = False):
        if not self.dirty and not rewrite:
            return False
        if not self.deSynced and not rewrite:
            if self.rewrite_interval == 0 or time.time() - os.path.getmtime(self._fileName) < self.rewrite_interval:
                return False
        try:
            if self.verbose:
                self.__teePrintOrNot(f"Rewriting {self._fileName}")
            self.clear_file()
            self.appendQueue.clear()
            self.appendQueue.extend(['\t'.join(self[key]) for key in self])
            if self.verbose:
                self.__teePrintOrNot(f"{len(self)} records appended to appendQueue for {self._fileName}")
            if not self.appendThread.is_alive():
                self.commitAppendToFile()
            # else:
            #     self.appendEvent.set()
            return True
        except Exception as e:
            self.__teePrintOrNot(f"Failed to write to {self._fileName}: {e}",'error')
            self.deSynced = True
            return False
    
    def _appendWorker(self):
        while not self.shutdownEvent.is_set():
            self.sync()
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
                file = self.get_file_obj('a')
                while self.appendQueue:
                    line = self.appendQueue.popleft()
                    file.write(line+'\n')
                if self.verbose:
                    self.__teePrintOrNot(f"Records commited to {self._fileName}")
                self.release_file_obj(file)
            except Exception as e:
                self.__teePrintOrNot(f"Failed to write to {self._fileName}: {e}",'error')
                self.deSynced = True

    def stopAppendThread(self):
        self.sync(rewrite=self.rewrite_on_exit)  # Ensure any final sync operations are performed
        # self.appendEvent.set()
        self.shutdownEvent.set()  # Signal the append thread to shut down
        self.appendThread.join()  # Wait for the append thread to complete 
        if self.verbose:
            self.__teePrintOrNot(f"Append thread for {self._fileName} stopped")
    
    def get_file_obj(self,modes = 'a'):
        self.writeLock.acquire()
        try:
            file = open(self._fileName, mode=modes, encoding='utf8')
            # Lock the file after opening
            if os.name == 'posix':
                fcntl.lockf(file, fcntl.LOCK_EX)
            elif os.name == 'nt':
                # For Windows, locking the entire file, avoiding locking an empty file
                lock_length = max(1, os.path.getsize(self._fileName))
                msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, lock_length)
            if self.verbose:
                self.__teePrintOrNot(f"File {self._fileName} locked")
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
