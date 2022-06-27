"""CD Chat server program."""
import logging, socket, selectors
from src.protocol import CDProto,JoinMessage,TextMessage

logging.basicConfig(filename="server.log", level=logging.DEBUG)

class Server:
    """Chat Server process."""
    def __init__(self) -> None:
        #Setup da Socket do Server
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("localhost",9000))
        self.server_socket.listen()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #Setup do Selector
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.server_socket, selectors.EVENT_READ, self.accept)

        self.channels = {"main": []}

    def loop(self):
        """Loop indefinetely."""
        while True:
            events = self.sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)

    def accept(self, sock):
        conn, addr = sock.accept()
        #conn.setblocking(False)
        msg = CDProto.recv_msg(conn)
        self.channels["main"].append(conn)
        logging.debug(f"Received -> {msg}")
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read (self,conn):
        msg = CDProto.recv_msg(conn)
        logging.debug(f"Received -> {msg}")

        if msg:
            if isinstance(msg, JoinMessage):
                self.channels.setdefault(msg.channel, [])
                for key in self.channels.keys():
                    if conn in self.channels[key]:
                        self.channels[key].remove(conn)
                self.channels[msg.channel].append(conn)
            elif isinstance(msg, TextMessage):
                if msg.channel is None: msg.channel = "main"
                for client in self.channels[msg.channel]:
                    CDProto.send_msg(client, msg)
                    logging.debug(f"Sent -> {str(msg)}")
        else:
            for key in self.channels.keys():
                if conn in self.channels[key]:
                    self.channels[key].remove(conn)
            self.sel.unregister(conn)
            conn.close()