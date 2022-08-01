from src.protocol import Protocol
import pickle, io
from PIL import Image
from time import sleep
from random import randint
Image.MAX_IMAGE_PIXELS = None
class Worker:
    def __init__(self, port:int, broker:int) -> None:
        self.address:tuple[str,int] = ("localhost", port)
        self.broker:tuple[str,int] = ("localhost", broker)
        self.protocol = Protocol(self.address)
        self.protocol.send_data(self.broker, {"type": "JW"})

    def run(self):
        while True:
            msg = self.protocol.recv_data()
            if not msg:
                continue
            msg = pickle.loads(msg["recv_data"])
            if msg["type"] == "RSZ":
                print(f"[WORKER] Recepcionado {msg['img_name']}")
                sleep(randint(0,5))
                img = Image.open(io.BytesIO(msg["image"]))
                aspect_ratio = img.size[0]/img.size[1]
                new_height = msg["size"]
                new_width = int(new_height * aspect_ratio)
                img = img.resize((new_width, new_height))
                buffer = io.BytesIO()
                img.save(buffer, format="PNG")
                self.protocol.send_data(self.broker, {"type": "RDONE","image": buffer.getvalue(), "time": msg["time"], "img_name": msg["img_name"] })
                print(f"[WORKER] Redimensionamento completado")
            elif msg["type"] == "STC":
                sleep(randint(0,5))
                print(f"[WORKER] Recepcionado {msg['img1_name'].split('/')[-1]} e {msg['img2_name'].split('/')[-1]} para colagem")
                img1 = msg["img1"]
                img2 = msg["img2"] 

                img1 = Image.open(io.BytesIO(img1))
                x1, y1 = img1.size

                img2 = Image.open(io.BytesIO(img2))
                x2, y2 = img2.size

                nx, ny = x1+x2, y1
                px, py = x1, 0
               
                result = Image.new('RGBA', (nx, ny))
                result.paste(img1, (0, 0))
                result.paste(img2, (px, py))
                buffer = io.BytesIO()
                result.save(buffer, format="PNG")
                self.protocol.send_data(self.broker, {"type": "SDONE","image": buffer.getvalue(),"time": msg["time"],"img1_name": msg["img1_name"],"img2_name": msg["img2_name"] })
                print(f"[WORKER] Colagem completada")
            elif msg["type"] == "QUIT":
                print("Fui de férias")
                self.protocol.send_data(self.broker, {"type": "NOICE"})
                exit(0)
