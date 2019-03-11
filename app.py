from flask import Flask
from onesource import create_and_run_job


app = Flask(__name__)


@app.route('/process', methods=['POST'])
def process():
    create_and_run_job('/var/data/in', '/var/data/out', '/var/data/temp', overwrite=True, delete=True)


if __name__ == '__main__':
    app.run()
