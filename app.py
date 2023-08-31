import json
import os
from flask import Flask, flash, redirect, request, send_from_directory, url_for
from werkzeug.utils import secure_filename

from onesource import create_and_run_job


UPLOAD_FOLDER = '/var/data/in'
DOWNLOAD_FOLDER = '/var/data/out/tika_extract'
ALLOWED_EXTENSIONS = {'pdf', 'docx'}

DEBUG = os.getenv('DEBUG', 'false') == 'true'


app = Flask(__name__)
app.secret_key = b'one source for file extraction'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.add_url_rule(
    "/uploads/<name>", endpoint="download_file", build_only=True
)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/uploads/<name>')
def download_file(name):
    return send_from_directory(app.config["DOWNLOAD_FOLDER"], name)


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    print('request: ' + str(request))
    if request.method == 'POST':
        print('request.files: ' + str(request.files))
        # check if the post request has the file part
        if 'file' not in request.files:
            print('No file part')
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        print('file: ' + str(file))
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            print('No selected file')
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            process()
            with open('/var/data/temp/var-data-in.json') as f:
                job_info = json.load(f)
            output_path = job_info['tika_extract'][0]['path']
            output_file = os.path.basename(output_path)
            # url = url_for('download_file', name=output_file, _external=True, _scheme='https')
            url = url_for('download_file', name=output_file)
            print('url: ' + str(url))
            return redirect(url)
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


@app.route('/process', methods=['POST'])
def process():
    create_and_run_job('/var/data/in', '/var/data/out', '/var/data/temp', overwrite=True, delete=True)


if __name__ == '__main__':
    if DEBUG:
        app.run(debug=True, port=5001)
    else:
        app.run()
