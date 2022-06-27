import argparse
import pickle
import socket
import io
from PIL import Image
from pprint import pprint

from src.protocol import Protocol

if __name__ == '__main__':
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

    while True:
        inp = input("> ")

        if inp == "peers":
            proto.send_node_list()
            size = int.from_bytes(conn.recv(12), byteorder="big")
            if not size:
                break
            d = pickle.loads(conn.recv(size))
            print(d["args"]["peers"])
        elif inp.startswith("get"):
            key = inp.split(" ")[1]
            proto.send_get_image(key, args.port)
            size = int.from_bytes(conn.recv(12), byteorder="big")
            if not size:
                break
            d = b"";

            while len(d) < size:
                d += conn.recv(size - len(d))

            d = pickle.loads(d)
            if d["args"]["image"]:
                out = io.BytesIO()
                out.write(d["args"]["image"])
                Image.open(out).show()
            else:
                print("Image not found")

        elif inp == "list":
            proto.send_get_image_location()
            size = int.from_bytes(conn.recv(12), byteorder="big")
            if not size:
                break
            d = pickle.loads(conn.recv(size))
            pprint(d["args"]["locations"])
            print("SIZE: ", len(d["args"]["locations"]))

        elif inp == "map":
            proto.send_get_image_by_node()
            size = int.from_bytes(conn.recv(12), byteorder="big")
            if not size:
                break
            d = pickle.loads(conn.recv(size))
            pprint(d)
        elif inp == "exit":
            print("GOODBYE")
            exit(0)
        else:
            print("Unknown command")
            continue