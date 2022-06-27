"""Message Broker"""
import selectors
import socket
import enum
from typing import Dict, List, Any, Tuple
from src import protocol

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000

        self.broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.broker_socket.bind((self._host,self._port)) 
        self.broker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broker_socket.listen()
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.broker_socket, selectors.EVENT_READ, self.accept)


        #topic -> {"/test": [(clSock1, Pickle), (clSock2, json)...], "/asss"}
        self.topics = {}
        self.posts = {}

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        ret = []
        post_names = self.posts.keys()
        for p in post_names:
            if self.posts[p] != "":
                ret.append(p)
        
        return ret

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.posts:
            return self.posts[topic]
            

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics.setdefault(topic, list())
        self.posts[topic] = value

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        return self.topics[topic]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        self.topics.setdefault(topic,list())
        self.topics[topic].append((address,_format))
     
    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        self.topics[topic] = list(filter(lambda sub : sub[0] != address, self.topics[topic]))

    def read(self, sock):
        header = sock.recv(8)

        if not header:
            for topic, val in self.topics.items():
                if sock in [v[0] for v in val]:
                    self.unsubscribe(topic,sock)
            self.sel.unregister(sock)
            sock.close()
            return

        header = int.from_bytes(header,byteorder="big")
        msg_type = sock.recv(2)
        msg_type = int.from_bytes(msg_type,byteorder="big")
        data = sock.recv(header)
        msg = None
        if msg_type == 0:
            msg = protocol.fromJSON(data)
        elif msg_type == 1:
            msg = protocol.fromXML(data)
        else: 
            msg = protocol.fromPickle(data)
        


        if msg["type"] == "subscribe":
            self.subscribe(msg["topic"], sock, msg_type)

            if msg_type == 0:
                sock.send(protocol.toJSON(protocol.SubscribeAck(msg["topic"],self.posts.get(msg["topic"], ""))))
            elif msg_type == 1:
                sock.send(protocol.toXML(protocol.SubscribeAck(msg["topic"],self.posts.get(msg["topic"], ""))))
            else:
                sock.send(protocol.toPickle(protocol.SubscribeAck(msg["topic"],self.posts.get(msg["topic"], ""))))

        elif msg["type"] == "publish":
            self.put_topic(msg["topic"],msg["post"])

            topics = msg["topic"].split("/")
            for i in range(len(topics)):
                if "/".join(topics[:i+1]) in self.topics.keys():
                    for sub_socket, sub_type in self.topics["/".join(topics[:i+1])]:
                        if sub_type == 0:
                            sub_socket.send(protocol.toJSON(protocol.Notify(msg["topic"],msg["post"])))
                        elif sub_type == 1:
                            sub_socket.send(protocol.toXML(protocol.Notify(msg["topic"],msg["post"])))
                        else:
                            sub_socket.send(protocol.toPickle(protocol.Notify(msg["topic"],msg["post"])))


        elif msg["type"] == "cancel":
            self.unsubscribe(msg["topic"],sock)
            self.sel.unregister(sock)
            sock.close()

        elif msg["type"] == "list":
            if msg_type == 0:
                sock.send(protocol.toJSON(protocol.ListAck(self.list_topics())))
            elif msg_type == 1:
                sock.send(protocol.toXML(protocol.ListAck(self.list_topics())))
            else:
                sock.send(protocol.toPickle(protocol.ListAck(self.list_topics())))
    

    def accept(self, s):
        conn, addr = s.accept()
        self.sel.register(conn, selectors.EVENT_READ, self.read)
    
    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)

