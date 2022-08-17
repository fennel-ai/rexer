import time

from absl import app
from absl import flags
from prometheus_client import start_http_server

FLAGS = flags.FLAGS

flags.DEFINE_integer('metrics_port', 8002, 'Port to run the metrics server on', lower_bound=0)


def main(_):
    start_http_server(FLAGS.metrics_port)
    # event loop
    while True:
        time.sleep(10)


if __name__ == '__main__':
    app.run(main)
