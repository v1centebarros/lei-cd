import pickle, socket, hashlib


class Protocol:
    def __init__(self, address: tuple[str, int]) -> None:
        self.address = address[0]+":"+str(address[1])
        self.counter: int = 0
        self.max_packet_size = 42069
        #create UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(address)
        self.sock.settimeout(None)
        self.msg_queue:list[dict] = [] 
        
    def __send_ack(self) -> None:
        size = int.to_bytes(0,8,"big")
        _type = int.to_bytes(1,1,"big")
        _hash = hashlib.sha1(self.address.encode("utf-8")).digest()[0].to_bytes(1,"big")
        counter = int.to_bytes(self.counter,2,"big")
        return size + _type + _hash + counter
    
    def __send_ack_resp(self) -> None:
        size = int.to_bytes(0,8,"big")
        _type = int.to_bytes(2,1,"big")
        _hash = hashlib.sha1(self.address.encode("utf-8")).digest()[0].to_bytes(1,"big")
        counter = int.to_bytes(self.counter,2,"big")
        return size + _type + _hash + counter
    
    def __send_data(self, packet, s):
        size = int.to_bytes(s,8,"big")
        _type = int.to_bytes(5,1,"big")
        _hash = hashlib.sha1(self.address.encode("utf-8")).digest()[0].to_bytes(1,"big")
        counter = int.to_bytes(self.counter,2,"big")
        return size + _type + _hash + counter + packet

    def send_data(self, to:tuple[str,int], message: dict):
        data = pickle.dumps(message)
        processed_packets = 0
        while processed_packets < len(data):
            packet = data[processed_packets:processed_packets+ self.max_packet_size - 12]
            processed_packets+=len(packet)
            self.sock.sendto(self.__send_data(packet, len(data)), to)
            self.sock.sendto(self.__send_ack(), to)

            received = False
            for i in range(4):
                try:
                    self.sock.settimeout(1)
                    payload,original = self.sock.recvfrom(42069)
                except socket.timeout:
                    continue
                msg_type = payload[8]
                if msg_type == 2:
                    received = True
                    break
                elif msg_type == 1:
                    self.sock.sendto(self.__send_ack_resp(), original)
                elif msg_type == 5:
                    if payload[9] not in [m["identification"] for m in self.msg_queue]:
                        self.msg_queue.insert(0, {"original": original,"recv_data": payload[12:],"identification":payload[9],"is_done":len(payload[12:]) == int.from_bytes(payload[:8], "big"), "count": int.from_bytes(payload[:8], "big")})
                    else:
                        msg = next(filter(lambda m: m["identification"] == payload[9], self.msg_queue))
                        msg["recv_data"] += payload[12:]
                        msg["is_done"] = len(msg["recv_data"]) == msg["count"]
            
            if not received:
                return

        self.counter+= 1
        self.counter%= 64 
        
    def recv_data(self):
        while len(self.msg_queue) == 0 or not self.msg_queue[-1]["is_done"]:
            try:
                self.sock.settimeout(4)
                payload,source = self.sock.recvfrom(42069)
            except socket.timeout:
                return None
            msg_type = payload[8]
            if msg_type == 2:
                pass
            elif msg_type == 1:
                self.sock.sendto(self.__send_ack_resp(), source)
            elif msg_type == 5:
                if payload[9] not in [m["identification"] for m in self.msg_queue]:
                    self.msg_queue.insert(0, {"original": source,"recv_data": payload[12:],"identification":payload[9],"is_done":len(payload[12:]) == int.from_bytes(payload[:8], "big"), "count": int.from_bytes(payload[:8], "big")})
                else:
                    msg = next(filter(lambda m: m["identification"] == payload[9], self.msg_queue))
                    msg["recv_data"] += payload[12:]
                    msg["is_done"] = len(msg["recv_data"]) == msg["count"]
        return self.msg_queue.pop()