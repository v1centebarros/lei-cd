"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
import selectors
from src import protocol
import socket

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type =_type
        
        self._host = "localhost"
        self._port = 5000

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self._host, self._port))

        if self.type == MiddlewareType.CONSUMER:
            self.sock.send(self.serialize(protocol.Subscribe(self.topic)))
            header = self.sock.recv(8)
            _ = self.sock.recv(2)
            msg = self.sock.recv(int.from_bytes(header, byteorder="big"))
        else:
            self.sel = selectors.DefaultSelector()
            self.sel.register(self.sock, selectors.EVENT_READ, self.pull)
        
    def serialize(self, value):
        raise NotImplemented

    def deserialize(self, value):
        raise NotImplemented

    def push(self, value):
        """Sends data to broker."""
        self.sock.send(self.serialize(protocol.Publish(self.topic,value)))

    def pull(self):
        """
        Receives (topic, data) from broker.
        Should BLOCK the consumer!
        """
        header = self.sock.recv(8)
        _ = self.sock.recv(2)
        msg = self.sock.recv(int.from_bytes(header, byteorder="big"))
        msg = self.deserialize(msg)

        return self.topic, msg["post"]

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""

    def cancel(self):
        """Cancel subscription."""


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def serialize(self, value):
        return protocol.toJSON(value)
    
    def deserialize(self, value):
        return protocol.fromJSON(value)

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def serialize(self, value):
        return protocol.toXML(value)
    
    def deserialize(self, value):
        return protocol.fromXML(value)

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def serialize(self, value):
        return protocol.toPickle(value)
    
    def deserialize(self, value):
        return protocol.fromPickle(value)
