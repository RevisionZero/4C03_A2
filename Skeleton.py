#!/usr/bin/python3
#==============================================================================
#description     :This is a skeleton code for programming assignment 
#usage           :python Skeleton.py trackerIP trackerPort
#python_version  :>= 3.5
#Authors         :Yongyong Wei, Rong Zheng
#==============================================================================

import socket, sys, threading, json,time,os,ssl
import os.path
import glob
import json
import optparse
import math

ignored_extensions = ['.so', '.py', '.dll']

def validate_ip(s):
    """
    Validate the IP address of the correct format
    Arguments: 
    s -- dot decimal IP address in string
    Returns:
    True if valid; False otherwise
    """
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True

def validate_port(x):
    """Validate the port number is in range [0,2^16 -1 ]
    Arguments:
    x -- port number
    Returns:
    True if valid; False, otherwise
    """
    if not x.isdigit():
        return False
    i = int(x)
    if i < 0 or i > 65535:
            return False
    return True

def get_file_info():
    """Get file info in the local directory (subdirectories are ignored).
    Return: a JSON array of {'name':file,'mtime':mtime}
    i.e, [{'name':file,'mtime':mtime},{'name':file,'mtime':mtime},...]
    Hint: a. you can ignore subfolders, *.so, *.py, *.dll, and this script
          b. use os.path.getmtime to get mtime, and round down to integer
    """
    file_arr = []
    #YOUR CODE
    
    for f in os.listdir('.'):
        name, extension = os.path.splitext(f)
        if os.path.isfile(f) and extension not in ignored_extensions:
            file_arr.append(
                {
                    "name":f,
                    "mtime":math.floor(os.path.getmtime(f))
                }
            )

    return file_arr

def get_files_dic():
    """Get file info as a dictionary {name: mtime} in local directory.
    Hint: same filtering rules as get_file_info().
    """
    file_dic = {}
    #YOUR CODE
    for f in os.listdir('.'):
        name, extension = os.path.splitext(f)
        if os.path.isfile(f) and extension not in ignored_extensions:
            file_dic[f] = math.floor(os.path.getmtime(f))

    return file_dic

def check_port_avaliable(check_port):
    """Check if a port is available
    Arguments:
    check_port -- port number
    Returns:
    True if valid; False otherwise
    """
    if str(check_port) in os.popen("netstat -na").read():
        return False
    return True
	
def get_next_avaliable_port(initial_port):
    """Get the next available port by searching from initial_port to 2^16 - 1
       Hint: You can call the check_port_avaliable() function
             Return the port if found an available port
             Otherwise consider next port number
    Arguments:
    initial_port -- the first port to check

    Return:
    port found to be available; False if no port is available.
    """

    #YOUR CODE

    for i in range(initial_port, 65535):
        if check_port_avaliable(i):
            return i
    return False


class FileSynchronizer(threading.Thread):
    def __init__(self, trackerhost,trackerport,port, host='0.0.0.0'):

        threading.Thread.__init__(self)

        #Own port and IP address for serving file requests to other peers
        self.port = port
        self.host = host

        #Tracker IP/hostname and port
        self.trackerhost = trackerhost
        self.trackerport = trackerport

        self.BUFFER_SIZE = 8192

        #Create a TCP socket to communicate with the tracker
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.settimeout(180)
        self._tracker_buf = b''

    
        #Store the message to be sent to the tracker. 
        #Initialize to the Init message that contains port number and file info.
        #Refer to Table 1 in Instructions.pdf for the format of the Init message
        #You can use json.dumps to conver a python dictionary to a json string
	#Encode using UTF-8
        self.msg = json.dumps(
            {
                "port": self.port,
                "files": get_file_info()
            }
        ).encode('utf-8') + b'\n'

        #Create a TCP socket to serve file requests from peers.
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.server.bind((self.host, self.port))
        except socket.error:
            print(('Bind failed %s' % (socket.error)))
            sys.exit()
        self.server.listen(10)

    def fatal_tracker(self, message, exc=None):
        """Abort the process on tracker failure"""
        if exc is not None:
            print((message, exc))
        else:
            print(message)
        try:
            self.server.close()
        except Exception:
            pass
        os._exit(1)


    # Not currently used. Ensure sockets are closed on disconnect
    def exit(self):
        self.server.close()

    #Handle file request from a peer(i.e., send the file content to peers)
    def process_message(self, conn, addr):
        '''
        Arguments:
        self -- self object
        conn -- socket object for an accepted connection from a peer
        addr -- IP address of the peer (only used for testing purpose)
        '''
        #YOUR CODE
        #Step 1. read the file name terminated by '\n'
        msg = b''
        while b'\n' not in msg:
            chunk = conn.recv(self.BUFFER_SIZE)
            if not chunk:
                break
            msg += chunk
        f,msg = msg.split(b'\n',1)
        f = f.decode('utf-8')
        #Step 2. read content of that file in binary mode
        binary = ''
        with open(f, 'rb') as file:
            binary = file.read()
        #Step 3. send header "Content-Length: <size>\n" then file bytes
        conn.send(f"Content-Length: {len(binary)}\n".encode('utf-8'))
        conn.send(binary)
        #Step 4. close conn when you are done.
        conn.close()

    def run(self):
        #Step 1. connect to tracker; on failure, may terminate
        #YOUR CODE
        self.client.connect((self.trackerhost,self.trackerport))
        t = threading.Timer(2, self.sync)
        t.start()
        print(('Waiting for connections on port %s' % (self.port)))
        while True:
            #Hint: guard accept() with try/except and exit cleanly on failure
            try:
                conn, addr = self.server.accept()
            except Exception as e:
                print(('Accept failed %s' % (e)))
                sys.exit()
            threading.Thread(target=self.process_message, args=(conn,addr)).start()

    #Send Init or KeepAlive message to tracker, handle directory response message
    #and  request files from peers
    def sync(self):
        print(('connect to:'+self.trackerhost,self.trackerport))
        #Step 1. send Init msg to tracker (Note init msg only sent once)
        #Since self.msg is already initialized in __init__, you can send directly
        #Hint: on send failure, may terminate
        #YOUR CODE
        try:
            self.client.send(self.msg)
        except Exception as e:
            print(('Send init msg failed %s' % (e)))
            sys.exit()

        #Step 2. now receive a directory response message from tracker
        directory_response_message = ''
        #Hint: read from socket until you receive a full JSON message ending with '\n'
        #YOUR CODE
        directory_response_message = b''
        while b'\n' not in directory_response_message:
            chunk = self.client.recv(self.BUFFER_SIZE)
            if not chunk:
                break
            directory_response_message += chunk

        # Split at first newline
        directory_response_message, d = directory_response_message.split(b'\n', 1)
        directory_response_message = directory_response_message.decode('utf-8')
        print('received from tracker:',directory_response_message)

        #Step 3. parse the directory response message. If it contains new or
        #more up-to-date files, request the files from the respective peers.
        #NOTE: compare the modified time of the files in the message and
        #that of local files of the same name.
        #Hint: a. use json.loads to parse the message from the tracker
        #      b. read all local files, use os.path.getmtime to get the mtime 
        #         (also note round down to int)
        #      c. for new or more up-to-date file, you can call syncfile()
        #      d. use Content-Length header to know file size
        #      e. if transfer fails, discard partial file
        #      f. finally, write the file content to disk with the file name, use os.utime
        #         to set the mtime
        #YOUR CODE
        cloud_files = json.loads(directory_response_message)
        local_files = get_files_dic()
        for file in cloud_files:
            if file not in local_files or cloud_files[file]["mtime"] > local_files[file]:
                # TO-DO: What's the mtime arg that needs to be passed in to synfile, the latest time??
                self.syncfile(file,cloud_files[file])

        #Step 4. construct a KeepAlive message
        #Note KeepAlive msg is sent multiple times, the format can be found in Table 1
        #use json.dumps to convert python dict to json string.
        self.msg = json.dumps({"port":self.port}).encode('utf-8') + b'\n'

        #Step 5. start timer
        t = threading.Timer(5, self.sync)
        t.start()

    def syncfile(self, filename, file_dic):
        """Fetch a file from a peer and store it locally.

        Arguments:
        filename -- file name to request
        file_dic -- dict with 'ip', 'port', and 'mtime'
        """
        #YOUR CODE
        #Step 1. connect to peer and send filename + '\n'
        peer_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        temp_filename = filename + ".part"
        try:
            peer_socket.connect((file_dic["ip"],file_dic["port"]))
            peer_socket.send(f"{filename}\n".encode())
            #Step 2. read header "Content-Length: <size>\n"
            # Read each byte individually instead of using a buffer to prevent overshoot into
            # into the actual file data
            content_len = b''
            while True:
                chunk = peer_socket.recv(1)
                if not chunk:
                    break
                content_len += chunk
                if chunk == b'\n':
                    break
            content_len = content_len.decode('utf-8')
            size = int(content_len.split(":")[1].strip())
            #Step 3. read exactly <size> bytes; if short, discard partial file
            #Step 4. write file to disk (binary), rename from .part when done
            bytes_recieved = 0
            with open(temp_filename, 'wb') as f:
                for i in range(size):
                    chunk = peer_socket.recv(1)
                    if not chunk:
                        break
                    bytes_recieved += 1
                    f.write(chunk)
            # Double check successful file reciept
            if bytes_recieved != size:
                os.remove(temp_filename)
                return
            os.rename(temp_filename, filename)
            # TO-DO: what is .part?
            # TO-DO: what should I set access time(atime) to in os.utime?
            #Step 5. set mtime using os.utime
            os.utime(path=filename,times=(file_dic["mtime"], file_dic["mtime"]))
        except Exception as e:
            print(f"Transfer failed for {filename}. Discarding.")
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
        finally:
            peer_socket.close()

if __name__ == '__main__':
    parser = optparse.OptionParser(usage="%prog ServerIP ServerPort")
    try:
        options, args = parser.parse_args()
    except SystemExit:
        sys.exit(1)

    if len(args) < 1:
        parser.error("No ServerIP and ServerPort")
    elif len(args) < 2:
        parser.error("No  ServerIP or ServerPort")
    else:
        if validate_ip(args[0]) and validate_port(args[1]):
            tracker_ip = args[0]
            tracker_port = int(args[1])

            # get a free port
            synchronizer_port = get_next_avaliable_port(8000)
            synchronizer_thread = FileSynchronizer(tracker_ip,tracker_port,synchronizer_port)
        else:
            parser.error("Invalid ServerIP or ServerPort")
