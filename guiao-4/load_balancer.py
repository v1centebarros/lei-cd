# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None


# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


#Todo DONE round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.currentServer = -1

    def select_server(self):
            if (self.currentServer%len(self.servers)+1) == len(self.servers):
                self.currentServer = 0
            else:
                self.currentServer = self.currentServer%len(self.servers)+1
            return self.servers[self.currentServer]
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.connections = {i[1]: 0 for i in self.servers}
    def select_server(self):
        selected_server = min(self.connections, key=self.connections.get)
        self.connections[selected_server] += 1
        for i in self.servers:
            if i[1] == selected_server:
                return i
                
    def update(self, *arg):
        self.connections[arg[0][1]] -= 1

# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.currentServer=-1
        self.server_time = {server[1]: 0 for server in self.servers}
        self.avg_time = {server[1]: 0 for server in self.servers}
        self.historio={server[1]: [] for server in self.servers}

    def select_server(self):
        while True:
            selected_server = min(self.avg_time.values())
            if (self.currentServer%len(self.servers)+1) == len(self.servers):
                self.currentServer = 0
            else:
                self.currentServer = self.currentServer%len(self.servers)+1
            
            if(self.server_time[self.servers[self.currentServer][1]] == selected_server):
                break

        self.server_time[selected_server] = time.time()
        return self.servers[self.currentServer]

    def update(self, *arg):
        self.server_time[arg[0][1]] = time.time() - self.server_time[arg[0][1]]

        for server in self.historio:
                self.historio[server].append(self.server_time[server])
        for server in self.servers:
            sum_tempos =  sum(self.historio[server[1]])
            self.avg_time[server[1]] = sum_tempos/len(self.historio[server[1]])
       

POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock

    def delete(self, sock):
        paired_sock = self.get_sock(sock)
        sel.unregister(sock)
        sock.close()
        sel.unregister(paired_sock)
        paired_sock.close()
        if sock in self.map:
            self.map.pop(sock)
        else:
            self.map.pop(paired_sock)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                if(key.fileobj.fileno()>0):
                    callback = key.data
                    callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
