from src.worker import Worker
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port',type=int, help='port')
    parser.add_argument('-b', '--broker',type=int, default=42069, help='broker port')

    args = parser.parse_args()
    Worker(args.port, args.broker).run()