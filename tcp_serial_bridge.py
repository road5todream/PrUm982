import argparse
import serial
import socket
import select
import sys
import time


def run_bridge(serial_port: str, serial_baud: int, tcp_host: str, tcp_port: int) -> None:
    ser = None
    listen_sock = None
    client_sock = None

    try:
        ser = serial.Serial(
            port=serial_port,
            baudrate=serial_baud,
            timeout=0.1,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
        )
        print(f"Serial: {serial_port} @ {serial_baud}")

        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listen_sock.bind((tcp_host, tcp_port))
        except OSError as e:
            if e.errno == 48 or "already in use" in str(e).lower():  # 48 = Address already in use (macOS)
                print(f"Порт {tcp_port} занят. Укажите другой: --tcp-port 4000 или --tcp-port 5001", file=sys.stderr)
            raise
        listen_sock.listen(1)
        listen_sock.setblocking(False)
        print(f"TCP: {tcp_host}:{tcp_port} (ожидание подключения...)")

        while True:
            if client_sock is None:
                try:
                    client_sock, addr = listen_sock.accept()
                    client_sock.setblocking(False)
                    print(f"TCP клиент подключён: {addr}")
                except BlockingIOError:
                    time.sleep(0.2)
                    continue

            r, _, _ = select.select([ser, client_sock], [], [], 0.5)
            try:
                if ser in r and ser.in_waiting:
                    data = ser.read(ser.in_waiting)
                    if data and client_sock:
                        client_sock.sendall(data)
                if client_sock and client_sock in r:
                    data = client_sock.recv(4096)
                    if not data:
                        client_sock.close()
                        client_sock = None
                        print("TCP клиент отключён")
                        continue
                    ser.write(data)
                    ser.flush()
            except (ConnectionResetError, BrokenPipeError, OSError):
                if client_sock:
                    try:
                        client_sock.close()
                    except Exception:
                        pass
                    client_sock = None
                    print("TCP клиент отключён")

    except serial.SerialException as e:
        print(f"Ошибка последовательного порта: {e}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        if e.errno == 48 or "already in use" in str(e).lower():
            print(f"\nИспользуйте свободный порт: --tcp-port 4000 или -t 5001", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nОстановка моста...")
    finally:
        if client_sock:
            try:
                client_sock.close()
            except Exception:
                pass
        if listen_sock:
            try:
                listen_sock.close()
            except Exception:
                pass
        if ser and ser.is_open:
            ser.close()
        print("Мост остановлен.")


def main():
    ap = argparse.ArgumentParser(
        description="Мост TCP <-> Serial для тестирования UM982 по TCP при подключении устройства по USB."
    )
    ap.add_argument(
        "--serial-port", "-s",
        required=True,
        help="Последовательный порт (например /dev/cu.usbserial-0001 или COM3)",
    )
    ap.add_argument(
        "--baudrate", "-b",
        type=int,
        default=460800,
        help="Скорость порта (по умолчанию 460800)",
    )
    ap.add_argument(
        "--tcp-port", "-t",
        type=int,
        default=4000,
        help="TCP порт для прослушивания (по умолчанию 4000; если занят — укажите другой, напр. 5001)",
    )
    ap.add_argument(
        "--tcp-host",
        default="0.0.0.0",
        help="Адрес для прослушивания TCP (по умолчанию 0.0.0.0 — все интерфейсы)",
    )
    args = ap.parse_args()
    run_bridge(args.serial_port, args.baudrate, args.tcp_host, args.tcp_port)


if __name__ == "__main__":
    main()
