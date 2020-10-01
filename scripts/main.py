import socket
import aws
import metrics
import argparse
import logging
import os


SOCKET_RECEIVE_BUFFER_SIZE = 4096
SOCKET_MAX_QUEUED_CONNECTION = 128

# A sidecar that forwards syslog data from a port (default: 10000) to cloduwatch log.
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
        "--metrics_port",
        help="port used to export metrics to prometheus",
        type=int,
        default=10001,
    )
    parser.add_argument(
        "--log_group",
        help="aws cloudwatch log group",
        type=str,
        default="mp_auditlog_group",
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

    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format='[%(asctime)s] [%(levelname)s] %(message)s'
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', args.port)
    sock.bind(server_address)
    sock.listen(args.max_queue)

    m = metrics.Metrics(args.metrics_port)

    while True: 
        # Establish connection with client. 
        logging.info("Wait for connection")
        connection, client_address = sock.accept()
        logging.info("Got connection from:" + str(client_address))
        cloudwatchlog = aws.CloudWatchLog(args.log_group, args.region)
        try:
            while True:
                # Receive data from client
                raw_data = connection.recv(args.buffer_size)
                if raw_data:
                    sent_success = cloudwatchlog.send_data(raw_data, m)
                    if sent_success:
                        m.cloudwatchlog_successed()
                    else:
                        logging.info("Fail to send data to cloudwatchlog.")
                        m.cloudwatchlog_failed()
                else:
                    # disconnect from client
                    break
        except Exception as e:
            logging.exception('Fail to send log to aws cloudwatch. ' + str(e))
        finally:
            logging.info("tcp connection closed.")
            # Clean up the connection
            connection.close()


if __name__ == "__main__":
    main()

