"""CD Chat client program"""
import logging,sys,socket, fcntl,os,selectors

from .protocol import CDProto, JoinMessage, RegisterMessage, TextMessage

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)


class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        self.name = name
        self.channel = None
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.client_socket, selectors.EVENT_READ, self.read)

    def read(self, conn):
        msg = CDProto.recv_msg(conn)
        if isinstance(msg, TextMessage):
            print(msg.message,end="")
            logging.debug(f"Received -> {str(msg)}")

    def connect(self):
        """Connect to chat server and setup stdin flags."""

        self.client_socket.connect(("localhost",9000))
        CDProto.send_msg(self.client_socket,RegisterMessage(self.name))
        logging.debug(f"REGISTER from {self.name}")

    def loop(self):
        """Loop indefinetely."""
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.got_keyboard_data)
        while True:
            #sys.stdout.write("->")
            #sys.stdout.flush()
            for key, _ in self.sel.select():
                callback = key.data
                callback(key.fileobj)

    def got_keyboard_data(self,stdin):
        input_msg = stdin.read()

        if input_msg.startswith("/join"):
            input_msg = input_msg.replace("/join","").rstrip()
            self.channel = input_msg
            msg = JoinMessage(input_msg)
            CDProto.send_msg(self.client_socket,msg)
            logging.debug(f"Sent -> {str(msg)}")
        elif input_msg.rstrip() == "exit":
            self.client_socket.close()
            exit()
        else:
            msg = TextMessage(input_msg,self.channel)
            CDProto.send_msg(self.client_socket,msg)
            logging.debug(f"Sent -> {str(msg)}")