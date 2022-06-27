from flask import Flask, render_template
import argparse
import pickle
import socket
import io
from PIL import Image
from pprint import pprint

from src.protocol import Protocol
app = Flask(__name__)

@app.route("/")
def map():
    proto.send_get_image_location()
    size = int.from_bytes(conn.recv(12), byteorder="big")
    if not size:
        return
    d = pickle.loads(conn.recv(size))
    return render_template("map.html", data=d["args"]["locations"],size=len(d["args"]["locations"]))

@app.route("/img/<key>")
def get_image(key):
    proto.send_get_image(key, args.port)
    size = int.from_bytes(conn.recv(12), byteorder="big")
    if not size:
        return
    d = b"";

    while len(d) < size:
        d += conn.recv(size - len(d))

    d = pickle.loads(d)
    if d["args"]["image"]:
        out = io.BytesIO()
        out.write(d["args"]["image"])
        Image.open(out).save("static/tmp.jpg")
        return render_template("image.html",data=key)
    else:
        return

if __name__ =="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--super', type=int, help='port of superpeer')
    parser.add_argument('-p', '--port', type=int, help='port of client')
    args = parser.parse_args()
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect(("127.0.0.1", args.super))
    except ConnectionRefusedError:
        print("ERROR: daemon.py not running")
        exit(1)

    proto = Protocol(conn, ("127.0.0.1", args.port))
    proto.send_register_client(("127.0.0.1", args.port))

    app.run(host="127.0.0.1", port=8080,debug=True)
    