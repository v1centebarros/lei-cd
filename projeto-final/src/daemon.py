import socket, os, selectors, pickle, io, sys
from typing import List
import imagehash
from pprint import pprint
from PIL import Image
from src.protocol import Protocol


class Daemon:
    
    def __init__(self,daemon_port, img_folder,super_port = None) -> None:
        self.addr = ("127.0.0.1", daemon_port)

        # Define the socket that will receive the connections
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.recv_socket.bind(("127.0.0.1", daemon_port))
        self.recv_socket.listen()

        # Define the socket that will link with the client
        self.client = None
        self.msg_count = 0
        self.msg_threshold = 5

        self.sel = selectors.DefaultSelector()
        self.sel.register(self.recv_socket, selectors.EVENT_READ, self.connection_request)

        #List of all peers in the network
        self.peers: List[Protocol] = []

        #Check if is the super node
        if super_port is not None:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect(("127.0.0.1", super_port))
            p = Protocol(conn, ("127.0.0.1", super_port))
            p.send_join(self.addr)
            self.peers.append(p)
            # p.send_get_map(self.addr)
            # p.send_get_folder_size(self.addr)

            self.sel.register(conn, selectors.EVENT_READ, self.disconnect)

        #Map of all the images in the network
        self.img_map = {}
        #Map of sizes of the folder of images in the peers
        self.folder_size_map = {}
        #Personal image folder
        self.img_folder = img_folder
        #Dictionary of all the personal images 
        self.personal_collection = {}
        self.remove_duplicates()

        self.network_map = {}
        print("===Recv Socket===")
        pprint(self.recv_socket)

    def connection_request(self, conn):
        """
        Handles a connection request.
        """
        
        s, _ = conn.accept()
        print("===Connection Request===")
        self.network_map[s] = None

        self.sel.register(s, selectors.EVENT_READ, self.read_request)
        
    def disconnect(self, conn: socket.socket):
        s = conn.recv(12)
        if len(s) == 0:
            print("===DEAD NODE===")
            proto = [p for p in self.peers if conn == p.send_sock]
            self.peers = [p for p in self.peers if p.send_sock != conn]
            if proto:
                proto = proto[0]              
                self.img_map = {k: [vi for vi in v if vi.send_sock != proto.send_sock] for k, v in self.img_map.items()}
                self.img_map = {k: v for k, v in self.img_map.items() if v}
                self.folder_size_map = {k: v for k, v in self.folder_size_map.items() if k != proto.address}
                
                for peer in self.peers:
                    peer.send_get_map(self.addr)
                    peer.send_get_folder_size(self.addr)
            else:
                print("Unknown dead node")

            self.sel.unregister(conn)
            conn.close()


            

    def read_request(self, conn: socket.socket):
        
        print("===FOLDER_SIZE_MAP===")
       
        s = conn.recv(12)
        #User Disconnected
        if len(s) == 0:
            self.sel.unregister(conn)
            conn.close()
            return


        size = int.from_bytes(s, byteorder="big")
        d = b"";

        while len(d) < size:
            d += conn.recv(size - len(d))

        d = pickle.loads(d)
        args = d["args"]

        #User Connected
        if d["type"] == "peer":
            print("===PEER CALL===")
            new_peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            new_peer.connect(args["addr"])
            self.peers.append(Protocol(new_peer, args["addr"]))
            self.peers[-1].send_get_map(self.addr)
            self.peers[-1].send_get_folder_size(self.addr)

            self.sel.register(new_peer, selectors.EVENT_READ, self.disconnect)
            
            self.msg_threshold+=5

        #Communicate that a new peer has joined
        elif d["type"] == "join":
            print("===JOIN CALL===")
            if args["addr"] not in [p.address for p in self.peers]:
                new_peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_peer.connect(args["addr"])
                new_peer_proto = Protocol(new_peer, args["addr"])
                for proto in self.peers: 
                    proto.send_peer(args["addr"])
                    new_peer_proto.send_peer(proto.address)
                self.peers.append(new_peer_proto)
                self.sel.register(new_peer, selectors.EVENT_READ, self.disconnect)
           
                self.msg_threshold+=5
                # new_peer_proto.send_get_folder_size(self.addr)

                for peer in self.peers:
                    peer.send_get_map(self.addr)
                    peer.send_get_folder_size(self.addr)

        #Create Connection with peer's client
        elif d["type"] == "register":
            print("===REGISTER CALL===")
            self.client = Protocol(conn, args["addr"])
        #Get a list of all the peers in the network
        elif d["type"] == "list_nodes":
            print("===LIST NODES CALL===")
            self.client.send_nodes(list(map(lambda p: p.address, self.peers)))
        
        #Handle a request to send a image to a client
        elif d["type"] == "get_image":
            print("===GET IMAGE CALL===")
            if args["key"] in self.personal_collection.keys():
                #If the direct daemon has the image
                if self.client and conn == self.client.send_sock:
                    self.client.send_image(self.personal_collection[args["key"]][1], self.addr)
                else:
                #Send the image to another daemon that made the request
                    peer = next(filter(lambda p: p.address == args["addr"], self.peers))
                    peer.send_image(self.personal_collection[args["key"]][1], self.addr)

            # If the image exists in the network ask the peer to send it    
            elif args["key"] in self.img_map:
                peer = self.img_map[args["key"]][0]
                peer.send_get_image(args["key"], self.addr)
            #If the image is not in the network, send empty bytes
            else:
                self.client.send_image(b'', self.addr)
        #Handle the request to send a image to the client
        elif d["type"] == "image":
            print("===IMAGE CALL===")
            p = self.client if args["addr"] else next(filter(lambda p: p.address == args["addr"], self.peers))
            p.send_image(args["image"], None)

        #Send image names to the client
        elif d["type"] == "get_map":
            print("===GET MAP CALL===")
            p = next(filter(lambda peer: peer.address == args["addr"], self.peers), self.client)
            p.send_map({k: [vi.address for vi in v] for k, v in self.img_map.items()}, self.addr)

       
        elif d["type"] == "map":
            print("===MAP CALL===")
            p = next(filter(lambda peer: peer.address == args["addr"], self.peers), self.client)
            for k,v in args["map"].items():
                for vi in v:
                    self.img_map.setdefault(k,[])
                    if vi != self.addr and not [i for i in self.img_map[k] if i.address == vi]:
                        peer = next(filter(lambda peer: peer.address == vi, self.peers))
                        self.img_map[k].append(peer)

            
            for k,v in self.img_map.items():
                if len(v) == 1 and self.addr != v[0].address and self.addr == min(self.folder_size_map.items(), key=lambda x: x[1])[0]:
                    self.img_map[k][0].send_get_duplication_image(self.addr, k)
                    self.img_map[k].append(Protocol(self.recv_socket, self.addr))

            for k,v in self.img_map.items():
                self.img_map[k] = list(set(v))


        elif d["type"] == "get_image_location":
            print("===GET IMAGE LOCATION CALL===")
            self.client.send_image_location(list(self.img_map.keys()))
        
        elif d["type"] == "get_folder_size":
            print("===GET FOLDER SIZE CALL===")
            p = next(filter(lambda peer: peer.address == args["addr"], self.peers), self.client)
            p.send_folder_size(self.addr, self.folder_size_map)
        
        elif d["type"] == "folder_size":
            print("===FOLDER SIZE CALL===")
            for k,v in args["size"].items():
                self.folder_size_map[k] = v

        elif d["type"] == "get_duplication_image":
            print("===GET DUPLICATION IMAGE CALL===")
            p = next(filter(lambda peer: peer.address == args["addr"], self.peers), self.client)
            p.send_duplication_image(self.addr, *self.personal_collection[args["key"]],args["key"])

        elif d["type"] == "duplication_image":
            print("===DUPLICATION IMAGE CALL===")
            self.personal_collection[args["key"]] = (args["hash"],args["image"])
            for peer in self.peers:
                peer.send_get_map(self.addr)
                peer.send_get_folder_size(self.addr)
        
        elif d["type"] == "get_image_by_node":
            print("===GET IMAGE BY NODE CALL===")
            self.client.send_image_by_node({k:[vi.address for vi in v] for k, v in self.img_map.items()})


        else:
            print("===UNKNOWN CALL===")
            print(d)
            
        if self.msg_count % self.msg_threshold == 0:
            for peer in self.peers:
                peer.send_get_map(self.addr)
                peer.send_get_folder_size(self.addr)
        self.msg_count += 1

    def remove_duplicates(self):
        for img_file in os.listdir(self.img_folder):
            if os.path.isfile(self.img_folder+"/"+img_file):
                if img := Image.open(self.img_folder+"/"+img_file):
                    output = io.BytesIO()
                    img.convert("RGB").save(output,format="jpeg")
                    
                    img_hash = imagehash.average_hash(img)
                    if img_hash not in [i[0] for i in self.personal_collection.values()]:
                        self.personal_collection[img_file.replace("%","")] = (img_hash, output.getvalue())
                        self.img_map[img_file.replace("%","")] = [Protocol(self.recv_socket, self.addr)]
                    else:
                        i = next(filter(lambda pair: self.personal_collection[pair][0] == img_hash, self.personal_collection.keys()))
                        if self.best_image(self.personal_collection[i], (img_hash, output.getvalue()),img_file.replace("%","")):
                            del self.img_map[i]
                            del self.personal_collection[i]
                            self.personal_collection[img_file.replace("%","")] = (img_hash, output.getvalue())
                            self.img_map[img_file.replace("%","")] = [Protocol(self.recv_socket, self.addr)]


                else:
                    print("ERROR! Could not open image")
        self.folder_size_map[self.addr] = get_size(self.personal_collection)
        
    def best_image(self, previous_img, new_img,key):
            out = io.BytesIO()
            out.write(previous_img[1])
            prev_img = Image.open(out)
            
            out = io.BytesIO()
            out.write(new_img[1])
            n_image = Image.open(out)
            prev_width, prev_height = prev_img.size
            n_width, n_height = n_image.size

            return n_width*n_height > prev_width*prev_height

    def run(self):
        while True:
            events = self.sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0

    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size