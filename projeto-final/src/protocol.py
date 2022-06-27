import pickle, socket
from typing import Dict

class Protocol:
    def __init__(self, sock, addr) -> None:
        self.send_sock = sock
        self.address = addr

    def __send(self, data: Dict) -> None:
        payload = pickle.dumps(data)
        payload = len(payload).to_bytes(12, byteorder='big') + payload
        self.send_sock.send(payload)
        
    def send_peer(self, addr):
        self.__send({'type': 'peer', "args": {"addr": addr}})
        
    def send_join(self, addr):
        self.__send({'type': 'join', "args": {"addr": addr}})
        
    def send_node_list(self):
        self.__send({'type': 'list_nodes', "args": {}})
        
    def send_nodes(self, peers):
        self.__send({'type': 'nodes', "args": {"peers": peers}})
        
    def send_register_client(self, addr):
        self.__send({'type': 'register', "args": {"addr": addr}})

    def send_image(self,image,addr):
        self.__send({'type': 'image', "args": {"image": image, "addr": addr}})

    def send_get_image(self,key,addr):
        self.__send({'type': 'get_image', "args": {"key": key, "addr": addr}})

    def send_get_map(self,addr):
        self.__send({'type': 'get_map', "args": {"addr": addr}})

    def send_map(self,_map,addr):
        self.__send({'type': 'map', "args": {"map": _map, "addr": addr}})

    def send_get_image_location(self):
        self.__send({'type': 'get_image_location', "args": {}})
        
        
    def send_image_location(self,locations):
        self.__send({'type': 'image_location', "args": {"locations": locations}})

    def send_get_folder_size(self,addr):
        self.__send({'type': 'get_folder_size', "args": {"addr": addr}})

    def send_folder_size(self,addr,size):
        self.__send({'type': 'folder_size', "args": {"size": size,"addr": addr}})

    def send_get_duplication_image(self,addr,key):
        self.__send({'type': 'get_duplication_image', "args": {"addr": addr,"key": key}})
    
    def send_duplication_image(self,addr,_hash,image,key):
        self.__send({'type': 'duplication_image', "args": {"addr": addr,"hash": _hash,"image": image,"key": key}})
    
    def send_get_image_by_node(self):
        self.__send({'type': 'get_image_by_node', "args": {}})
    
    def send_image_by_node(self,_map):
        self.__send({'type': 'image_by_node', "args": {"map":_map}})

    def __repr__(self) -> str:
        return self.send_sock.__repr__()

    def __hash__(self) -> int:
        return hash(self.address)
