import os
import tumult

from flask import Flask


# 1. Run `flask run`
# 2. Access http://127.0.0.1:5000/tmlt
def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    @app.route('/tmlt')
    def tmlt():
        members_df = tumult.add_spark_context_file()
        session = tumult.configure_session(members_df, "members", 3)
        count = tumult.count_query(session, "members", None, 1)
        return tumult.getShowString(count)

    return app
