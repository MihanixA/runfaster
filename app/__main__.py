import os
import re
import logging
from binascii import crc_hqx
from datetime import datetime
from flask import Flask, request, render_template, redirect
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired
from flask_bootstrap import Bootstrap
from gevent.pywsgi import WSGIServer
from google.cloud import spanner
from google.api_core.exceptions import AlreadyExists

secret_key = os.urandom(32)

app = Flask(__name__)
Bootstrap(app)

app.secret_key = secret_key.hex()

spanner_client = spanner.Client()

app_settings = os.environ.get('APP_SETTINGS')

instance_id = os.environ.get('SPANNER_INSTANCE', 'runfaster-spanner')
database_id = os.environ.get('SPANNER_DATABASE', 'runfaster')

database = spanner_client.instance(instance_id).database(database_id, ddl_statements=["""CREATE TABLE urls (
    shorten STRING(MAX) NOT NULL,
    created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    source STRING(MAX) NOT NULL,
) PRIMARY KEY (shorten)
"""])

source_regex = re.compile("https?://(?:[-\\w.]|(?:%[\\da-fA-F]{2}))+")
shorten_regex = re.compile("[a-zA-Z0-9]")


def get_source(shorten: str):
    if shorten_regex.match(shorten) is None:
        raise ValueError("wrong input")
    with database.snapshot() as snapshot:
        cursor = snapshot.execute_sql(
            f"SELECT source FROM urls WHERE shorten=@shorten",
            params={'shorten': shorten},
            param_types={'shorten': spanner.param_types.STRING}
        )
    results = list(cursor)
    return results[0][0]


def _generate_shorten(source: str):
    return hex(crc_hqx(source.encode(), 0))[2:]


def create_shorten(source: str):
    if source_regex.match(source) is None:
        raise ValueError("wrong input")
    shorten = _generate_shorten(source)
    try:
        with database.batch() as batch:
            batch.insert(
                table='urls',
                columns=('shorten', 'source', 'created_at'),
                values=[(shorten, source, datetime.utcnow())]
            )
    except AlreadyExists:
        ...
    return shorten


class UrlForm(FlaskForm):
    source = StringField('source', validators=[DataRequired()])


@app.route('/<string:shorten>', methods=['GET'])
def redirect_to_source(shorten: str):
    try:
        source = get_source(shorten)
        return redirect(source)
    except ValueError:
        return render_template('400.html'), 400
    except IndexError:
        return render_template('404.html'), 404
    except Exception:
        return render_template('500.html'), 500


@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        form = UrlForm()
        shorten = None
        if form.validate_on_submit():
            try:
                source = form.source.data
                shorten = create_shorten(source)
            except ValueError:
                return render_template('400.html'), 400
        return render_template('index.html', form=form, shorten=shorten, base_url=request.base_url)
    except Exception:
        return render_template('500.html')


def main():
    if app_settings == 'dev':
        app.logger.setLevel(logging.DEBUG)
        app.run(debug=True, host='localhost', port=8080)
    else:
        port = int(os.environ.get('PORT', '8080'))
        server = WSGIServer(('0.0.0.0', port), app)
        server.serve_forever()


if __name__ == '__main__':
    main()
