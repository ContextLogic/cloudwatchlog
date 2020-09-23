# stream name?? 2020-09-23
# instance id?? mongoproxy-bb7c6f859-k8s7t
# port?? 10000
# what if the log is not sent to aws cloudwatch? logging
# Right now mongoproxy send data using syslog format. May need to change it.

import socket
import aws
import argparse
import logging


SOCKET_RECEIVE_BUFFER_SIZE = 4096
SOCKET_MAX_QUEUED_CONNECTION = 128

def main():
    parser = argparse.ArgumentParser(
        description="Cloudwatch logging sidecar. Forward logs to aws cloudwatch."
    )
    parser.add_argument(
        "--port",
        help="port used to receive log",
        type=int,
        default=10000,
    )
    parser.add_argument(
        "--protocol",
        help="protocol used to receive log",
        type=str,
        default="tcp",
    )
    parser.add_argument(
        "--log_group",
        help="aws cloudwatch log group",
        type=str,
        default="mp_auditlog_group",
        required=True,
    )
    parser.add_argument(
        "--region",
        help="aws region",
        type=str,
        default="us-west-1",
    )
    parser.add_argument(
        "--buffer_size",
        help="socket receive buffer size",
        type=int,
        default=4096,
    )
    parser.add_argument(
        "--max_queue",
        help="socket max queued connection",
        type=int,
        default=128,
    )
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', args.port)
    sock.bind(server_address)
    sock.listen(args.max_queue)

    token = ""
    while True: 
        # Establish connection with client. 
        logging.info("Wait for connection")
        connection, client_address = sock.accept()
        logging.info("Got connection from:" + str(client_address))
        cloudwatchlog = aws.CloudWatchLog(args.log_group, args.region)
        try:
            # Receive the data in small chunks and retransmit it
            while True:
                raw_data = connection.recv(args.buffer_size)
                data = raw_data.split(" ")[-1]
                if data:
                    token = cloudwatchlog.update_stream_name(token)
                    if not token:    
                        token = cloudwatchlog.prepare_send()
                    token = cloudwatchlog.send_data(token, data)
                else:
                    logging.error("Don't receive data from client.")
                    break
        except Exception as e:
            logging.error(str(e))
        finally:
            logging.info("tcp connection closed.")
            # Clean up the connection
            connection.close()


if __name__ == "__main__":
    main()

