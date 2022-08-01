import os, io, uuid, pickle
from PIL import Image
from src.protocol import Protocol
from time import time,sleep 

Image.MAX_IMAGE_PIXELS = None

class Broker:
    def __init__(self, folder, height) -> None:
        self.folder = folder
        self.height = height
        self.images = []
        self.load_images()
        self.edited_images = []
        self.images_on_work = []
        self.edited_images_on_work = []
        self.workers = []
        self.protocol = Protocol(("localhost", 42069))
        self.kick_timer = {}
        self.metadata = {"rtotal":0,
                        "stotal":0,
                        "itime":time(),
                        "rperworker":{},
                        "sperworker":{},
                        "minstime":None,
                        "maxstime":None,
                        "avgstime":0,
                        "minrtime":None,
                        "maxrtime":None,
                        "avgrtime":0,
                        "workperworker":{},
        }
        for filename in os.listdir("temp"): os.remove("temp/"+filename)
                
    def load_images(self):
        for filename in os.listdir(self.folder): self.images.append((filename, os.stat(self.folder + '/' + filename).st_mtime))
        self.images = sorted(self.images, key=lambda x: x[1]) #ordenar imagens
        print(f"[BROKER] Imagens carregadas com sucesso {len(self.images)} imagens carregadas")

    def load_image(self,path):
        img = Image.open(os.path.join(path))
        output = io.BytesIO()
        img.convert("RGBA").save(output,format="PNG")
        return output.getvalue()

    def save_image(self,image):
        img = Image.open(io.BytesIO(image))
        newImgName = str(uuid.uuid4().hex)
        img.save(os.path.join("temp",newImgName+ ".png"))
        return "temp/"+newImgName+ ".png"

    def check_all_is_finished(self):
        for w in self.workers:
            if w[1] != 1:
                return False
        return True
        
    def update_worker_status(self, worker, status):
        for w in self.workers:
            if w[0] == worker:
                w[1] = status
                break
    
    def attribute_worker(self,data):
        for w in self.workers:
            if w[1] == 1:
                w[1] = 2
                self.protocol.send_data(w[0], data)
    

    def run(self):
        while True:
            msg = self.protocol.recv_data()
            for w, t in list(self.kick_timer.items()):
                if time() - t > 10:
                    self.images_on_work = [i for i in self.images_on_work if i[1] != w]
                    self.edited_images_on_work = [i for i in self.edited_images_on_work if i[1] != w]
                    del self.kick_timer[w]
                    self.workers = [ wk for wk in self.workers if wk[0] != w]
                    for w in self.workers:
                        if w[1] == 1:
                            if len(self.images) - len(self.images_on_work):
                                self.update_worker_status(w[0], 2)
                                print(f"[BROKER] Enviar {self.images[0][0].split('/')[-1]} para redimensionar no worker {worker}")
                                for img in self.images:
                                    if img not in [i for i,_ in self.images_on_work]:
                                        self.images_on_work.append((img,w[0]))
                                        self.protocol.send_data(w[0], {"type": "RSZ","image": self.load_image(self.folder+"/"+img[0]),"img_name":img[0], "size": self.height,"time":img[1]})
                                        break
                            else:
                                self.update_worker_status(w[0], 2)
                                print(f"[BROKER] Enviar {self.images[0][0].split('/')[-1]} para redimensionar no worker {worker}")
                                for img in self.images:
                                    if img not in [i for i,_ in self.images_on_work]:
                                        self.images_on_work.append((img,w[0]))
                                        self.protocol.send_data(w[0], {"type": "RSZ","image": self.load_image(self.folder+"/"+img[0]),"img_name":img[0], "size": self.height,"time":img[1]})
                                        break


            if not msg:
                continue
            worker = msg["original"]
            msg = pickle.loads(msg["recv_data"])
            if msg["type"] == "JW":
                print(f"[BROKER] O worker {worker} entrou em serviço")
                self.workers.append([worker,1])
                if len(self.images) -len(self.images_on_work) > 0:
                    for w in self.workers:
                        if w[1] == 1:
                            self.update_worker_status(w[0], 2)
                            print(f"[BROKER] Enviar {self.images[0][0].split('/')[-1]} para redimensionar no worker {worker}")
                            for img in self.images:
                                if img not in [i for i,_ in self.images_on_work]:
                                    self.kick_timer[w[0]] = time()
                                    self.images_on_work.append((img,w[0]))
                                    self.protocol.send_data(w[0], {"type": "RSZ","image": self.load_image(self.folder+"/"+img[0]),"img_name":img[0], "size": self.height,"time":img[1]})
                                    break
                elif len(self.edited_images) > 1:
                    for w in self.workers:
                        if w[1] == 1:
                            self.update_worker_status(w[0], 2)

                            for i in range(len(self.edited_images)-1):
                                if self.edited_images[i] not in [i for i,_ in self.edited_images_on_work] and self.edited_images[i+1] not in [i for i,_ in self.edited_images_on_work]:
                                    self.kick_timer[w[0]] = time()

                                    img1,img2 = self.edited_images[i],self.edited_images[i+1]

                                    
                                    self.edited_images_on_work.append((img1, w[0]))
                                    self.edited_images_on_work.append((img2, w[0]))
        
                                    print(f"[BROKER] Enviar {img1[0].split('/')[-1]} e {img2[0].split('/')[-1]} para colagem no worker {worker}")
                                    self.protocol.send_data(w[0], {"type": "STC","img1": self.load_image(img1[0]), "img2": self.load_image(img2[0]),"img1_name":img1[0],"img2_name":img2[0], "time": img1[1] if img1[1] < img2[1] else img2[1]})
                                    break
            
            elif msg["type"] == "RDONE":
                if worker not in self.kick_timer:
                    print("Foi-se")
                    continue
                self.metadata["rtotal"]+=1
                self.metadata["rperworker"].setdefault(worker,0)
                self.metadata["rperworker"][worker]+=1
                self.update_worker_status(worker, 1)
                t = time() - self.kick_timer.pop(worker)
                self.metadata["avgrtime"]+=t
                if not self.metadata["minrtime"] or t < self.metadata["minrtime"]:
                    self.metadata["minrtime"] = t
                if not self.metadata["maxrtime"] or t > self.metadata["maxrtime"]:
                    self.metadata["maxrtime"] = t

                self.images = [x for x in self.images if x[0] != msg["img_name"]]
                self.images_on_work = [i for i in self.images_on_work if i[0][0] != msg["img_name"]]

                img_name = self.save_image(msg["image"])
                print(f"[BROKER] Recepcionado {img_name.split('/')[-1]} após redimensionamento no worker {worker}")
                self.edited_images.append((img_name,msg["time"]))
                self.edited_images = sorted(self.edited_images, key=lambda x: x[1]) 

                if len(self.images) - len(self.images_on_work) != 0:
                    for w in self.workers:
                        if w[1] == 1:
                            self.update_worker_status(w[0], 2)
                            for img in self.images:
                                if img not in [i for i,_ in self.images_on_work]:
                                    self.kick_timer[w[0]] = time()
                                    self.images_on_work.append((img,w[0]))
                                    print(f"[BROKER] Enviar {img[0].split('/')[-1]} para redimensionar no worker {w[0]}")
                                    self.protocol.send_data(w[0], {"type": "RSZ","image": self.load_image(self.folder+"/"+img[0]), "size": self.height, "img_name":img[0],"time": img[1]})
                                    break
                    
                elif len(self.edited_images) > 1:
                    for w in self.workers:
                        if w[1] == 1:
                            self.update_worker_status(w[0], 2)
                            
                            for i in range(len(self.edited_images)-1):
                                if self.edited_images[i] not in [i for i,_ in self.edited_images_on_work] and self.edited_images[i+1] not in [i for i,_ in self.edited_images_on_work]:
                                    self.kick_timer[w[0]] = time()

                                    img1,img2 = self.edited_images[i],self.edited_images[i+1]
                                    
                                    self.edited_images_on_work.append((img1,w[0]))
                                    self.edited_images_on_work.append((img2,w[0]))
        
                                    print(f"[BROKER] Enviar {img1[0].split('/')[-1]} e {img2[0].split('/')[-1]} para colagem no worker {worker}")
                                    self.protocol.send_data(w[0], {"type": "STC","img1": self.load_image(img1[0]), "img2": self.load_image(img2[0]),"img1_name":img1[0],"img2_name":img2[0],"time": img1[1] if img1[1] < img2[1] else img2[1]})
                                    break

            elif msg["type"] == "SDONE":
                if worker not in self.kick_timer:
                    print("Foi-se")
                    continue

                self.metadata["stotal"]+=1
                self.metadata["sperworker"].setdefault(worker,0)
                self.metadata["sperworker"][worker]+=1
                self.update_worker_status(worker, 1)
                t = time() - self.kick_timer.pop(worker)
                self.metadata["avgstime"]+=t
                if not self.metadata["minstime"] or t < self.metadata["minstime"]:
                    self.metadata["minstime"] = t
                if not self.metadata["maxstime"] or t > self.metadata["maxstime"]:
                    self.metadata["maxstime"] = t

                print(f"[BROKER] Recepcionado {img_name.split('/')[-1]} após colagem no worker {worker}")
                self.edited_images = list(filter(lambda x: x[0] != msg["img1_name"],self.edited_images))
                self.edited_images = list(filter(lambda x: x[0] != msg["img2_name"],self.edited_images))
                self.edited_images_on_work = list(filter(lambda x: x[0][0] != msg["img1_name"],self.edited_images_on_work))
                self.edited_images_on_work = list(filter(lambda x: x[0][0] != msg["img2_name"],self.edited_images_on_work))
                self.edited_images.append((self.save_image(msg["image"]),msg["time"]))
                self.edited_images = sorted(self.edited_images, key=lambda x: x[1])
               
                if len(self.edited_images) > 1:
                    for w in self.workers:
                        if w[1] == 1 and len(self.edited_images) - len(self.edited_images_on_work)> 1:
                            self.update_worker_status(w[0], 2)

                            for i in range(len(self.edited_images)-1):
                                if self.edited_images[i] not in [i for i,_ in self.edited_images_on_work] and self.edited_images[i+1] not in [i for i,_ in self.edited_images_on_work]:
                                    self.kick_timer[w[0]] = time()
                            

                                    img1,img2 = self.edited_images[i],self.edited_images[i+1]
                                    self.edited_images_on_work.append((img1,w[0]))
                                    self.edited_images_on_work.append((img2,w[0]))
                                    print(f"[BROKER] Enviar {img1[0].split('/')[-1]} e {img2[0].split('/')[-1]} para colagem no worker {w[0]}")
                                    self.protocol.send_data(w[0], {"type": "STC","img1": self.load_image(img1[0]), "img2": self.load_image(img2[0]),"img1_name":img1[0],"img2_name":img2[0], "time": img1[1] if img1[1] < img2[1] else img2[1]})
                                    break

                elif len(self.edited_images_on_work) == 0:
                    print(f"[BROKER] Imagem está pronta pronta!")
                    img = Image.open(self.edited_images.pop(0)[0])
                    img.save("result.png")
                    for worker in self.workers: 
                        self.protocol.send_data(worker[0], {"type": "QUIT"})
                        sleep(2)
                    print(
                        f"""
Redimensionamentos totais: {self.metadata["rtotal"]}
Colagens totais: {self.metadata["stotal"]}
Tempo total: {time()-self.metadata["itime"]}
Redimensionamentos por Worker: {self.metadata["rperworker"]}
Colagens por Worker: {self.metadata["sperworker"]}
Tempo mínimo por redimensionamento: {self.metadata["minrtime"]}
Tempo máximo por redimensionamento: {self.metadata["maxrtime"]}
Tempo médio por redimensionamento: {self.metadata["avgrtime"]/self.metadata["rtotal"]}
Tempo mínimo por colagem: {self.metadata["minstime"]}
Tempo máximo por colagem: {self.metadata["maxstime"]}
Tempo médio por colagem: {self.metadata["avgstime"]/self.metadata["stotal"]}

                        """
                    )
                    break
            