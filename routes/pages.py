"""HTML page routes."""

from flask import Blueprint, render_template

pages_bp = Blueprint("pages", __name__)


@pages_bp.route("/")
def index():
    return render_template("index.html")


@pages_bp.route("/stock/<symbol>")
def stock_detail(symbol):
    return render_template("stock_detail.html", symbol=symbol.upper())


@pages_bp.route("/portfolio")
@pages_bp.route("/portfolio/<int:pid>")
def portfolio_page(pid=None):
    return render_template("portfolio.html", portfolio_id=pid)
