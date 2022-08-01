from src.broker import Broker
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--folder',type=str, help='images folder')
    parser.add_argument('-a', '--altura',type=int, default=100, help='altura da imagem')

    args = parser.parse_args()
    broker = Broker(args.folder, args.altura)

    broker.run()


