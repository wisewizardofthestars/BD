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
DATABASE_URL = "postgres://db:db@postgres/db"

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
@app.route("/products", methods=("GET",))
def products():
    """Show all products and suppliers."""
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            products = cur.execute(
                """
                SELECT product.SKU, product.name, product.price, product.description, supplier.name AS supplier_name
                FROM product
                LEFT JOIN supplier ON product.SKU = supplier.SKU
                ORDER BY product.SKU;
                """
            ).fetchall()
            log.debug(f"Found {cur.rowcount} products.")

    return render_template("products/index.html", products=products)


app.route("/products/register", methods=("GET", "POST"))
def register_product():

    if request.method == "POST":
        SKU = request.form["SKU"]
        product_name = request.form["product_name"]
        description = request.form["description"]
        price = request.form["price"]
        ean = request.form["ean"]

        error = None

        if not SKU:
            error = "SKU is required."

        if not product_name:
            error = "Product name is required."

        if not price:
            error = "Price is required."
        elif not price.isnumeric():
            error = "Price must be a numeric value."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO product (SKU, name, description, price, ean)
                        VALUES (%(SKU)s, %(name)s, %(description)s, %(price)s, %(ean)s);
                        """,
                        {
                            "SKU": SKU,
                            "name": product_name,
                            "description": description,
                            "price": price,
                            "ean": ean,
                        },
                    )
                conn.commit()
            return redirect(url_for("products"))

    return render_template("products/register.html")


@app.route("/products/<string:SKU>/remove", methods=("POST",))
def remove_product(SKU):
    """Remove a product and supplier."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                DELETE FROM product
                WHERE SKU = %(SKU)s;
                """,
                {"SKU": SKU},
            )
        conn.commit()

    return redirect(url_for("products"))

#b) Alterar preços de produtos e respectivas descrições

@app.route("/products/<string:SKU>/update", methods=("GET", "POST"))
def update_product(SKU):
    """Update product prices and descriptions."""

    if request.method == "POST":
        price = request.form["price"]
        description = request.form["description"]

        error = None

        if not price:
            error = "Price is required."
        elif not price.isnumeric():
            error = "Price must be a numeric value."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        UPDATE product
                        SET price = %(price)s, description = %(description)s
                        WHERE SKU = %(SKU)s;
                        """,
                        {
                            "SKU": SKU,
                            "price": price,
                            "description": description,
                        },
                    )
                conn.commit()
            return redirect(url_for("products"))

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            product = cur.execute(
                """
                SELECT SKU, name, description, price
                FROM product
                WHERE SKU = %(SKU)s;
                """,
                {"SKU": SKU},
            ).fetchone()
            log.debug(f"Found product {SKU}: {product}")

    return render_template("products/update.html", product=product)

# ------------------- Customers -------------------

@app.route("/customers", methods=("GET",))
def customers():
    """Show all customers."""
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            customers = cur.execute(
                """
                SELECT cust_no, name
                FROM customer
                ORDER BY cust_no;
                """
            ).fetchall()
            log.debug(f"Found {cur.rowcount} customers.")

    return render_template("customers/index.html", customers=customers)


@app.route("/customers/register", methods=("GET", "POST"))
def register_customer():
    """Register a new customer."""
    if request.method == "POST":
        name = request.form["name"]
        email = request.form["email"]
        phone = request.form["phone"]
        address = request.form["address"]

        error = None

        if not name:
            error = "Customer name is required."

        if not email:
            error = "Email is required."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO customer (name, email, phone, address)
                        VALUES (%(name)s, %(email)s, %(phone)s, %(address)s);
                        """,
                        {"name": name, "email": email, "phone": phone, "address": address},
                    )
                conn.commit()
            return redirect(url_for("customers"))

    return render_template("customers/register.html")


@app.route("/customers/<int:cust_no>/remove", methods=("POST",))
def remove_customer(cust_no):
    """Remove a customer."""
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                DELETE FROM customer
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )
        conn.commit()

    return redirect(url_for("customers"))



# ------------------- Orders -------------------
@app.route("/orders", methods=("GET",))
def orders():
    """Show all orders."""
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            orders = cur.execute(
                """
                SELECT contains.order_no, product.name, contains.qty, product.price * contains.qty AS total_price
                FROM contains
                INNER JOIN product ON contains.SKU = product.SKU
                ORDER BY contains.order_no;
                """
            ).fetchall()
            log.debug(f"Found {cur.rowcount} orders.")

    return render_template("orders/index.html", orders=orders)


@app.route("/orders/place", methods=("GET", "POST"))
def place_order():
    """Place a new order."""
    if request.method == "POST":
        SKU = request.form["SKU"]
        qty = request.form["qty"]

        error = None

        if not SKU:
            error = "Product SKU is required."

        if not qty:
            error = "Quantity is required."
        elif not qty.isnumeric():
            error = "Quantity must be a numeric value."

        if error is not None:
            flash(error)
        else:
            # Calculate the total price based on the product's price and quantity
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        SELECT price
                        FROM product
                        WHERE SKU = %(SKU)s;
                        """,
                        {"SKU": SKU},
                    )
                    product_price = cur.fetchone()["price"]
                    total_price = float(product_price) * int(qty)

                    cur.execute(
                        """
                        INSERT INTO contains (order_no, SKU, qty, total_price)
                        VALUES (
                            (SELECT MAX(order_no) + 1 FROM orders),
                            %(SKU)s,
                            %(qty)s,
                            %(total_price)s
                        );
                        """,
                        {
                            "SKU": SKU,
                            "qty": qty,
                            "total_price": total_price,
                        },
                    )
                conn.commit()
            return redirect(url_for("orders"))

    return render_template("orders/place.html")


@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run()
