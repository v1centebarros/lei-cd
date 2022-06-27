import json, pickle
from typing import Dict
from xml.etree.ElementTree import Element,tostring, fromstring

class Message: 
    def __init__(self, command):
        self.command = command
    
    def toDict (self):
        return {"type": self.command}

class Subscribe(Message):
    def __init__(self, topic):
        super().__init__("subscribe")
        self.topic = topic

    def toDict (self):
        dict = super().toDict()
        dict.update({"topic": self.topic})
        return dict

class SubscribeAck(Message):
    def __init__(self, topic,last_post):
        super().__init__("subsack")
        self.topic = topic
        self.last_post = last_post

    def toDict (self):
        dict = super().toDict()
        dict.update({"topic": self.topic, "last_post":self.last_post})
        return dict


class Publish(Message):
    def __init__(self, topic, post):
        super().__init__("publish")
        self.topic = topic
        self.post = post
    
    def toDict (self):
        dict = super().toDict()
        dict.update({"topic":self.topic,"post":self.post})
        return dict

class Cancel(Message):
    def __init__(self, topic):
        super().__init__("cancel")
        self.topic = topic

    def toDict (self):
        dict = super().toDict()
        dict.update({"topic": self.topic})
        return dict

class List(Message):
    def __init__(self):
        super().__init__("list")
    
class ListAck(Message):
    def __init__(self,topics):
        super().__init__("listack")
        self.topics = topics
    
    def toDict (self):
        dict = super().toDict()
        dict.update({"topics":self.topics})
        return dict


class Notify(Message):
    def __init__(self,topic, post):
        super().__init__("notify")
        self.topic = topic
        self.post = post
        
    def toDict (self):
        dict = super().toDict()
        dict.update({"topic":self.topic,"post":self.post})
        return dict


def toXML(data : Dict) -> str: 
    elem = Element("message")
    for key, val in data.toDict().items():
        child = Element(key)
        child.text = str(val)
        elem.append(child)
    return len(tostring(elem,"utf-8")).to_bytes(8,byteorder="big") + int.to_bytes(1,2,byteorder="big") + tostring(elem,"utf-8")
 
def fromXML(data : str) -> Dict : 
    dict = {}
    for child in fromstring(data.decode("utf-8")):
        dict[child.tag] = child.text
    return dict    

# Convert JSON to Bytes
# fromJSON String -> Dicionário
def fromJSON(data : Dict) -> str:
    return json.loads(data.decode("utf-8"))

# Convert Bytes to JSON
def toJSON(data : str) -> Dict:
    dict = json.dumps(data.toDict()).encode("utf-8")
    return len(dict).to_bytes(8,byteorder="big") + int.to_bytes(0,2,byteorder="big") + dict

def fromPickle(data : Dict) -> str:
    return pickle.loads(data)

def toPickle(data : str) -> Dict:
    dict = pickle.dumps(data.toDict())
    return len(dict).to_bytes(8,byteorder="big") + int.to_bytes(2,2,byteorder="big") + dict