# from flask import Flask, render_template
# import psycopg2


# app = Flask(__name__)

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/register_products')
# def register_products():
#     return render_template('register_products.html')

# @app.route('/modify_prices')
# def modify_prices():
#     return render_template('modify_prices.html')

# @app.route('/register_customers')
# def register_customers():
#     return render_template('register_customers.html')

# @app.route('/place_orders')
# def place_orders():
#     return render_template('place_orders.html')

# @app.route('/simulate_payment')
# def simulate_payment():
#     return render_template('simulate_payment.html')

# # app.config['DATABASE'] = {
# #     'host': 'DOCKER_CONTAINER_IP_ADDRESS',
# #     'port': 'DOCKER_CONTAINER_PORT',
# #     # other configuration options
# # }

# # conn = psycopg2.connect(
# #     host=app.config['DATABASE']['host'],
# #     port=app.config['DATABASE']['port'],
# #     # other connection parameters
# # )

# # app.config['DATABASE'] = {
# #     'host': 'localhost',       # Update with your Docker container IP address if necessary
# #     'port': '5432',            # Default PostgreSQL port
# #     'database': 'your_db',     # Name of your PostgreSQL database
# #     'user': 'your_user',       # Username for the database
# #     'password': 'your_pass'    # Password for the database
# # }

# # def get_db():
# #     if 'db' not in g:
# #         g.db = psycopg2.connect(
# #             host=app.config['DATABASE']['host'],
# #             port=app.config['DATABASE']['port'],
# #             database=app.config['DATABASE']['database'],
# #             user=app.config['DATABASE']['user'],
# #             password=app.config['DATABASE']['password']
# #         )
# #     return g.db

# # @app.teardown_appcontext
# # def close_db(error):
# #     if hasattr(g, 'db'):
# #         g.db.close()

# def create_db_connection():
#     connection = psycopg2.connect(
#         host="localhost",
#         port="5432 ",
#         user="postgres ",
#         password="db",
#         database="db"
#     )
#     return connection

# # Example usage:
# connection = create_db_connection()

# if __name__ == '__main__':
#     app.run(debug=True)

#!/usr/bin/python3
from logging.config import dictConfig

import psycopg
from flask import flash
from flask import Flask
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool


# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = "postgres://db:db@localhost/db"

pool = ConnectionPool(conninfo=DATABASE_URL)
# the pool starts connecting immediately.

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
log = app.logger


@app.route("/", methods=("GET",))
@app.route("/accounts", methods=("GET",))
def account_index():
    """Show all the accounts, most recent first."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            accounts = cur.execute(
                """
                SELECT account_number, branch_name, balance
                FROM account
                ORDER BY account_number DESC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(accounts)

    return render_template("account/index.html", accounts=accounts)


@app.route("/accounts/<account_number>/update", methods=("GET", "POST"))
def account_update(account_number):
    """Update the account balance."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            account = cur.execute(
                """
                SELECT account_number, branch_name, balance
                FROM account
                WHERE account_number = %(account_number)s;
                """,
                {"account_number": account_number},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    if request.method == "POST":
        balance = request.form["balance"]

        error = None

        if not balance:
            error = "Balance is required."
            if not balance.isnumeric():
                error = "Balance is required to be numeric."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        UPDATE account
                        SET balance = %(balance)s
                        WHERE account_number = %(account_number)s;
                        """,
                        {"account_number": account_number, "balance": balance},
                    )
                conn.commit()
            return redirect(url_for("account_index"))

    return render_template("account/update.html", account=account)


@app.route("/accounts/<account_number>/delete", methods=("POST",))
def account_delete(account_number):
    """Delete the account."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                DELETE FROM account
                WHERE account_number = %(account_number)s;
                """,
                {"account_number": account_number},
            )
        conn.commit()
    return redirect(url_for("account_index"))


@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run()
