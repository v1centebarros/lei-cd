from src.daemon import Daemon
import argparse
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port',  type=int,  default=9000, help='port to listen on')
    parser.add_argument('-s', '--super', type=int, default=None, help='port of superpeer')
    parser.add_argument('-f', '--folder',type=str,              help='images folder')
    args = parser.parse_args()

    Daemon(daemon_port=args.port,img_folder=args.folder, super_port=args.super).run()