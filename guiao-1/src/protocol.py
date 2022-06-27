"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""
    def __init__(self,command) -> None:
        self.command = command

    def __repr__(self) -> str:
        return json.dumps({"command" : self.command })

    def len(self) -> int:
        return len(str(self))
    
class JoinMessage(Message):
    """Message to join a chat channel."""
    def __init__(self,channel) -> None:
        super().__init__("join")
        self.channel = channel
    
    def __repr__(self) -> str:
        return json.dumps({"command": self.command, "channel": self.channel})

class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self,user) -> None:
        super().__init__("register")
        self.user = user
    def __repr__(self) -> str:
        return json.dumps({"command": self.command, "user": self.user})
    
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self, message, channel=None, ts=None) -> None:
        super().__init__("message")
        self.message = message
        self.channel = channel
        if ts == None: 
            self.ts = int(datetime.now().timestamp())
        else:
            self.ts = ts

    def __repr__(self) -> str:
        if self.channel is None :
            return json.dumps({"command": self.command, "message": self.message,"ts":self.ts})
        else:
            return json.dumps({"command": self.command, "message": self.message, "channel":self.channel,"ts":self.ts})
class CDProto:
    """Computação Distribuida Protocol."""
    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage(username)
        
    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(channel)
        
    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage(message,channel)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message) -> None:
        """Sends through a connection a Message object."""
        connection.send(msg.len().to_bytes(2,byteorder="big") + str(msg).encode('utf-8'))

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        size_msg = int.from_bytes(connection.recv(2),'big')

        if size_msg == 0:
            return
        
        bytes_msg = connection.recv(size_msg)
        msg = bytes_msg.decode('utf-8')

        try:
            msg = json.loads(msg)
        except Exception:
            raise CDProtoBadFormat(bytes_msg)

        if "command" not in msg.keys():
            raise CDProtoBadFormat(bytes_msg)

        if msg["command"] == "join":
            if "channel" not in msg.keys():
                raise CDProtoBadFormat(bytes_msg)
            return JoinMessage(msg["channel"])
        
        elif msg["command"] == "register":
            if "user" not in msg.keys():
                raise CDProtoBadFormat(bytes_msg)
            return RegisterMessage(msg["user"])
        
        elif msg["command"] == "message":
            if "message" not in msg.keys() or "ts" not in msg.keys():
                raise CDProtoBadFormat(bytes_msg)

            if "channel" in msg.keys():
                return TextMessage(msg["message"],channel=msg["channel"], ts=int(msg["ts"]))
            else:
                return TextMessage(msg["message"],ts=int(msg["ts"]))

class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""
    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")